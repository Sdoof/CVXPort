
import time
import asyncio
import zmq
import threading


def server():
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind('tcp://127.0.0.1:12345')

    for _ in range(10):
        socket.send_string('AAPL 1234')
        socket.send_string('MSFT 123')
        time.sleep(0.1)


def client():
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect('tcp://127.0.0.1:12345')
    socket.subscribe('AAPL')
    for _ in range(4):
        print(socket.recv_string())

    socket.subscribe('MSFT')
    for _ in range(8):
        print(socket.recv_string())


threads = [threading.Thread(target=job) for job in [server, client]]
[t.start() for t in threads]
[t.join() for t in threads]
