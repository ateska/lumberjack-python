# Lumberjack protocol from LogStash and Beats for Python

A quick code dump of implementation of the Lumberjack protocol from LogStash and Beats.
No 3rd party dependencies.

## Proof Of Concept 

🚧🚧🚧 Only a proof-of-concept 🚧🚧🚧 in a **fully working** condition.


## How to Run it

The PoC is a standalone TCP server, implemented using `asyncio`.

```
% python ./lmio-ljpoc.py 
Serving on tcp/('0.0.0.0', 5044)
New connection from ('10.17.162.9', 59154)
...
```

The PoC dumps a separated JSON for every log event sent over the Lumberjack protocol from Winlogbeats and Filebeats.
Other beats are likely supported as well b/c they all use libbeats and https://github.com/elastic/go-lumber but I haven't tested them (yet).

SSL is not yet supported but it will be very simple to add (using TeskaLabs LogMan.io collector internal capabilities), including the SSL autodetection (so that people can mix encrypted and raw connections from Beats).

Beats `output` configuration:

```
output.logstash:
  enabled: true
  hosts: ["127.0.0.1:5044"]
```

Note: Change your IP address accordingly.

---

Contact [me](https://github.com/ateska) , if you have a question to this code.

