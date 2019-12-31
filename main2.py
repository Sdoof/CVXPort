
import zmq
import zmq.asyncio as azmq
import asyncio
import time
import threading
from datetime import datetime


def server():
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind('tcp://127.0.0.1:12345')
    time.sleep(0.1)
    for _ in range(5):
        socket.send_string('appleoh')
        time.sleep(0.2)
    socket.send_string('apple close')
    print('server done')


def client():
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.subscribe('apple ')
    socket.connect('tcp://127.0.0.1:12345')
    for _ in range(3):
        msg = socket.recv_string()
        print(msg)
        time.sleep(0.2)
    print('client done')


threads = [threading.Thread(target=func) for func in [server, client]]
[t.start() for t in threads]
[t.join() for t in threads]