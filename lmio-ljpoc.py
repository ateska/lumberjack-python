import zlib
import struct
import asyncio
import tempfile

# This is a Proof of Concept, we - for sure - don't need to handle all frame types.
#
# Based on: https://github.com/elastic/go-lumber/blob/main/client/v2/client.go
#
# Built by Ales Teska & a bit of AI assistance.


LUMBERJACK_VERSION_V2 = ord('2')


async def send_ack(writer, seq):
	"""Sends an acknowledgment frame to the client."""
	ack_frame = struct.pack("!BcI", LUMBERJACK_VERSION_V2, b'A', seq)
	writer.write(ack_frame)
	await writer.drain()


async def handle_frame_stream(reader, writer, addr):
	'''
	Handles the frame stream from the client.
	This is recursive, b/c the decompressed data is itself a frame stream.
	'''
	while True:

		# Read Lumberjack Protocol Header
		try:
			data = await reader.readexactly(2)
		except asyncio.IncompleteReadError:
			# Client disconnected / end of file
			return

		if not data:
			print(f"Client {addr} disconnected.")
			return

		(version, frame_type) = struct.unpack("!BB", data)

		if version != LUMBERJACK_VERSION_V2:
			print(f"Unsupported protocol version: {version}")
			return

		match frame_type:

			case 0x57:  # 'W' - 'window size' frame type
				data = await reader.readexactly(4)  # 32bit unsigned window size value
				(window_size,) = struct.unpack("!I", data)
				print(f'window size: {window_size}')

			case 0x43:  # 'C' - 'compressed' frame type
				data = await reader.readexactly(4)  # 32bit unsigned payload length value
				(payload_length,) = struct.unpack("!I", data)

				if payload_length > 16 * 1024 * 1024:
					# Little bit of protection against decompression bombs.
					print(f"Payload length is too large: {payload_length}")
					return

				# Read the compressed data into a temporary file (decompressed data)
				with tempfile.TemporaryFile() as f:
					decompressor = zlib.decompressobj()
					while payload_length > 0:
						data = await reader.readexactly(min(4096, payload_length))
						decompressed_data = decompressor.decompress(data)
						f.write(decompressed_data)
						payload_length -= len(data)

					decompressed_data = decompressor.flush()
					f.write(decompressed_data)

					# Iterate over the decompressed data (recursively)
					await handle_frame_stream(AsyncFileReader(f), writer, addr)

			case 0x4A:  # 'J' - 'JSON' frame type
				data = await reader.readexactly(8)
				(seq, payload_length) = struct.unpack("!II", data)

				if payload_length > 16 * 1024 * 1024:
					# Little bit of protection against decompression bombs.
					print(f"Payload length is too large: {payload_length}")
					return

				data = await reader.readexactly(payload_length)
				print(f'JSON data: read {len(data)} bytes.')

				# Bingo!
				# Here is the main payload, the JSON formatted structured data.
				# File `json_data.json` is overridden over and over in this version.
				with open('json_data.json', 'wb') as f:
					f.write(data)

				# The protocol requires an acknowledgment frames, here and there
				# It can skip as many ack frames as it wants, as long as the last frame contains a total number of items in the payload (highest seq number).
				# This can be futher optimized to reduce the number of ack frames sent.
				await send_ack(writer, seq)

			case _:
				print(f"Unknown/not supported frame, version: {version}, type: 0x{frame_type:02X} - drop a new issue here: https://github.com/ateska/lumberjack-python/issues please!")
				break


async def handle_client(reader, writer):
	# We received a new connection
	# Each connection is handled in a separate coroutine,
	# so we can serve multiple clients concurrently.

	addr = writer.get_extra_info('peername')
	print(f"New connection from {addr}")

	try:
		await handle_frame_stream(reader, writer, addr)

	except Exception as e:
		print(f"Error: {e}")

	# ... and we are done, close the connection
	finally:
		writer.close()
		await writer.wait_closed()

		print(f"Connection {addr} has been closed.")


async def main():
	# Listen on all interfaces, port 5044 (Logstash default port)
	server = await asyncio.start_server(handle_client, '0.0.0.0', 5044)

	addr = server.sockets[0].getsockname()
	print(f'Serving on tcp/{addr}')

	async with server:
		await server.serve_forever()


class AsyncFileReader:
	'''
	Embarasingly simple async file reader that reads from a temporary file.
	It enables (by NOT forcing me to implement two slightly different dispatchers)
	the recursive handling of the decompressed data, as it is itself a frame stream.

	Not sure if the tempfile.TemporaryFile is the best choice here, but it works for now.
	The Linux tempfile mechanism is powerful, let's use it.

	And yes, OS read() calls can block a little bit, but it's not a problem for now.
	'''

	def __init__(self, file: tempfile.TemporaryFile):
		"""
		:param file: Path to the file to be read
		"""
		self._file = file
		self._file.seek(0)

	async def read(self, n: int = -1) -> bytes:
		"""
		Reads up to `n` bytes from the file.
		:param n: Number of bytes to read (-1 for all available).
		:return: Read data.
		"""
		if n < 0:
			return self._file.read()
		return self._file.read(n)

	async def readline(self) -> bytes:
		"""Reads a line from the file."""
		return self._file.readline()

	async def readexactly(self, n: int) -> bytes:
		"""
		Reads exactly `n` bytes from the file.
		:param n: Number of bytes to read.
		:return: Read data.
		"""
		data = self._file.read(n)
		if len(data) < n:
			raise asyncio.IncompleteReadError(data, n)
		return data


if __name__ == '__main__':
	asyncio.run(main())
