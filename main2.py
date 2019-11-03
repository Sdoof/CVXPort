
import zmq.asyncio as azmq
import zmq
import asyncio


async def client():
    context = azmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect('tcp://127.0.0.1:5557')
    socket.setsockopt_string(zmq.SUBSCRIBE, 'usd')

    for x in range(5):
        msg = await socket.recv_string()
        print(f'[client]    {msg}')


async def server():
    context = azmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind('tcp://127.0.0.1:5557')

    await asyncio.sleep(0.8)
    print('server starts')
    for x in range(10):
        await socket.send_string(f'usd {x}')
        print(f'[server]    {x}')
        await asyncio.sleep(1e-5)
    print('server finishes')


async def main():
    await asyncio.gather(client(), server())


if __name__ == '__main__':
    asyncio.run(main())
