
# from cvxport.data import MT4DataObject
# from cvxport.const import Freq
import asyncio
import zmq
import zmq.asyncio as azmq


# data_obj = MT4DataObject(['usd', 'eur'], 12345)
# data_obj.set_params(Freq.TICK, 5)


# async def get_data():
#     res = []
#     counter = 0
#     async for data in data_obj():
#         res.append(data)
#         counter += 1
#         if counter > 3:
#             break
#
#     return res

async def client():
    context = azmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect('tcp://127.0.0.1:5557')
    socket.setsockopt_string(zmq.SUBSCRIBE, 'usd')

    print('client starts')
    for _ in range(5):
        msg = await socket.recv_string()
        print(f'[client]    {msg}')


async def server():
    context = azmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind('tcp://127.0.0.1:5557')

    await asyncio.sleep(0.01)
    print('server starts')
    for x in range(10):
        await socket.send_string(f'usd {x}')
        print(f'[server]    {x}')
        await asyncio.sleep(1)


async def main():
    res = await asyncio.gather(client(), server())
    print(res)


asyncio.run(main())
