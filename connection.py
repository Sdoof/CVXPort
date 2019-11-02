
import asyncio
import zmq
import zmq.asyncio as azmq


async def receiver():
    context = azmq.Context()
    in_socket = context.socket(zmq.SUB)
    in_socket.connect('tcp://localhost:32770')
    in_socket.setsockopt_string(zmq.SUBSCRIBE, 'EURUSD')

    while True:
        msg = await in_socket.recv_string()
        print(msg)
        print('sleep')
        await asyncio.sleep(5)
        print('wake up')


async def receiver2(id):
    context = azmq.Context()
    in_socket = context.socket(zmq.PULL)
    in_socket.connect('tcp://localhost:32769')

    while True:
        msg = await in_socket.recv_string()
        print(f'client{id}: {msg}')


async def sender():
    context = azmq.Context()
    out_socket = context.socket(zmq.PUSH)
    out_socket.connect('tcp://localhost:32768')

    # await out_socket.send_string(format_order(**{'action': 'OPEN'}))
    await out_socket.send_string(format_order(**{'action': 'CLOSE_MAGIC'}))

    for i in range(10):
        await out_socket.send_string(format_order(**{'action': 'GET_OPEN_TRADES'}))

    await out_socket.send_string(format_order(**{'action': 'CLOSE_ALL_MAGIC'}))


def format_order(action='OPEN', order_type=0, symbol='EURUSD', price=0.0, sl=50, tp=50, comment="Python-to-MT",
                 lots=0.01, magic=123456, ticket=0):
    return f'TRADE;{action};{order_type};{symbol};{price};{sl};{tp};{comment};{lots};{magic};{ticket}'


async def main():
    await asyncio.gather(receiver2(1), sender())

if __name__ == '__main__':
    asyncio.run(receiver())