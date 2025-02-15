import zlib
import struct
import asyncio
import tempfile


LUMBERJACK_VERSION_V2 = ord('2')


async def send_ack(writer, seq):
	"""Sends an acknowledgment frame to the client."""
	ack_frame = struct.pack("!BcI", LUMBERJACK_VERSION_V2, b'A', seq)
	writer.write(ack_frame)
	await writer.drain()


async def handle_frame_stream(reader, writer, addr):
	while True:

		# Read Lumberjack Protocol Header
		try:
			data = await reader.readexactly(2)
		except asyncio.IncompleteReadError:
			# Client disconnected
			break

		if not data:
			print(f"Client {addr} disconnected.")
			break

		(version, frame_type) = struct.unpack("!BB", data)

		if version != LUMBERJACK_VERSION_V2:
			print(f"Unsupported protocol version: {version}")
			break

		match frame_type:

			case 0x57:  # 'W' - 'window size' frame type
				data = await reader.readexactly(4)  # 32bit unsigned window size value
				(window_size,) = struct.unpack("!I", data)
				print(f'window size: {window_size}')

			case 0x43:  # 'C' - 'compressed' frame type
				data = await reader.readexactly(4)  # 32bit unsigned payload length value
				(payload_length,) = struct.unpack("!I", data)

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

					# Iterate over the decompressed data
					await handle_frame_stream(AsyncFileReader(f), writer, addr)

			case 0x4A:  # 'J' - 'JSON' frame type
				data = await reader.readexactly(8)
				(seq, payload_length) = struct.unpack("!II", data)

				data = await reader.readexactly(payload_length)
				print(f'JSON data: read {len(data)} bytes.')
				with open('json_data.json', 'wb') as f:
					f.write(data)

				await send_ack(writer, seq)

			case _:
				print(f"Unknown frame, version: {version}, type: 0x{frame_type:02X}")
				break


async def handle_client(reader, writer):
	addr = writer.get_extra_info('peername')
	print(f"New connection from {addr}")
	try:
		await handle_frame_stream(reader, writer, addr)

	except Exception as e:
		print(f"Error: {e}")

	finally:
		writer.close()
		await writer.wait_closed()

		print(f"Connection {addr} has been closed.")


async def main():
	server = await asyncio.start_server(handle_client, '0.0.0.0', 5044)

	addr = server.sockets[0].getsockname()
	print(f'Serving on {addr}')

	async with server:
		await server.serve_forever()


class AsyncFileReader:

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
