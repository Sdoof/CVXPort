
from cvxport.data import MT4DataObject
from cvxport.const import Freq
import pandas as pd
import asyncio
import zmq
import zmq.asyncio as azmq


async def client():
    data_obj = MT4DataObject(['usd', 'eur'], 12345)
    data_obj.set_params(Freq.TICK, 5)

    res = []
    counter = 0
    async for data in data_obj():
        res.append(data)
        print(res[-1][0])
        counter += 1
        if counter > 3:
            break

    return res


async def server():
    context = azmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind('tcp://127.0.0.1:12345')

    data = [
        'usd 1;1.1',
        'eur 2;2.05',
        'usd 1.1;1.15',
        'usd 1.15;1.2',
        'eur 2.05;2.1',
        'eur 2.05;2.1',
        'usd 1.05;1.1',
    ]

    await asyncio.sleep(1)
    print('server starts')
    for msg in data:
        await socket.send_string(msg)
        print(f'sent    {pd.Timestamp.now("UTC")}')
        await asyncio.sleep(0.1)
    print('server finishes')


async def main():
    res = await asyncio.gather(client(), server())
    return res

aaa = asyncio.run(main())
print(aaa)
