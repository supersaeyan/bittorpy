# bittorpy
Implemented a bep003 compliant Bit torrent engine in python as an attempt to learn Peer to Peer communication and gain experience in that paradigm. It supports http(s) trackers and implements Peer wire protocol.

The architecture is a variation of an Asynchronous Producer-Consumer model and can also be described as distributed publisher-subscriber with multiple topics where each piece downloaded from peers is added to respective file writer queue as a topic and the file writer coroutine is the subscriber for file writer queues.

It uses AsyncIO event loop as Network download and Disk writing are both IO blocking tasks, making it very efficient.
