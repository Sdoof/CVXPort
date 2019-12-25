
import zmq
import zmq.asyncio as azmq
import asyncio


channel1 = 'tcp://127.0.0.1:12345'
channel11 = 'tcp://127.0.0.1:12347'
channel2 = 'tcp://127.0.0.1:12346'


async def worker(id):
    # await asyncio.sleep(0.5)
    print(f'[Worker {id}] started')

    context = azmq.Context()
    out_socket = context.socket(zmq.PUSH)
    out_socket.connect(channel1)

    while True:
        await out_socket.send_string(f'{id}')
        print(f'worker {id} send {id}')
        await asyncio.sleep(3)


async def newcomer():
    await asyncio.sleep(0.5)
    print('[Newcomer] started')
    context = azmq.Context()

    in_socket = context.socket(zmq.PULL)
    in_socket.bind(channel1)

    while True:
        msg = await in_socket.recv_string()
        print(f'[Newcomer] get {msg}')

import inspect

def main():
    workers = [worker(i) for i in range(3)]
    comer = newcomer()
    print(inspect.iscoroutine(comer))
    print(inspect.iscoroutinefunction(newcomer))
    print(newcomer)
    return workers, comer
    # await asyncio.gather(comer, *workers)


if __name__ == '__main__':
    # asyncio.run(main())
    res = main()
    print('here')
