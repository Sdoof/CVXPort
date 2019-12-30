
import zmq
import zmq.asyncio as azmq
import asyncio
import time
from datetime import datetime


async def data_server():
    await asyncio.sleep(1)
    context = azmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind('tcp://127.0.0.1:12345')
    now = datetime.now()

    data = [{'date': now.strftime('%Y-%m-%d %H:%M:%S'), 'open': 1.12345, 'high': 1.1245, 'low': 1.1123, 'close': 1.1124}]
    for _ in range(100):
        await socket.send_string(str(data))
        await asyncio.sleep(0.005)
    await socket.send_string('close')
    socket.close()


async def client():
    context = azmq.Context()
    socket = context.socket(zmq.SUB)
    socket.setsockopt_string(zmq.SUBSCRIBE, '')
    socket.connect('tcp://127.0.0.1:12345')
    count = 0
    while True:
        msg = await socket.recv_string()
        if msg == 'close':
            break
        structure = eval(msg)
        count += 1
    print(count)
    socket.close()


async def main():
    await asyncio.gather(client(), data_server())

# asyncio.run(main())


def data_server():
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.setsockopt(zmq.SNDHWM, 0)
    socket.bind('tcp://127.0.0.1:12345')
    now = datetime.now()

    data = [{'date': now.strftime('%Y-%m-%d %H:%M:%S'), 'open': 1.12345, 'high': 1.1245, 'low': 1.1123, 'close': 1.1124}]
    for _ in range(100):
        socket.send_string(str(data))
        time.sleep(0.0001)
    socket.send_string('close')
    socket.close()

data_server()