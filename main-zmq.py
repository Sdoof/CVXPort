
import time
import asyncio
import zmq
import threading


def server():
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind('tcp://127.0.0.1:12345')

    for _ in range(2):
        msg = socket.recv_string()
        print(f'server {msg}')
        socket.send_string(msg)


def client(id):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect('tcp://127.0.0.1:6004')
    socket.subscribe('FX:EURUSD')
    for _ in range(10):
        print(f'client {socket.recv_string()}')


threads = [
    # threading.Thread(target=server),
    threading.Thread(target=client, args=(1,)),
    # threading.Thread(target=client, args=(2,)),
]
[t.start() for t in threads]
[t.join() for t in threads]
