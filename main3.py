
import time
import asyncio
import zmq
import threading


def server():
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind('tcp://127.0.0.1:12345')
    msg = socket.recv_json()
    print(f'server get {msg}')
    socket.send_json(msg)


def client():
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect('tcp://127.0.0.1:12345')
    socket.send_json({'abc': 123, 'oh': 'a'})
    msg = socket.recv_json()
    print(f'client gets {msg}')


threads = [threading.Thread(target=job) for job in [server, client]]
[t.start() for t in threads]
[t.join() for t in threads]
