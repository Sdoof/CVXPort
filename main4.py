import zmq
from multiprocessing import Process
import time
from datetime import datetime


def server():
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    # socket.setsockopt(zmq.SNDBUF, 10000)
    socket.set_hwm(51000)
    socket.bind('tcp://127.0.0.1:12345')
    now = datetime.now()

    data = [{'date': now.strftime('%Y-%m-%d %H:%M:%S'), 'open': 1.12345, 'high': 1.1245, 'low': 1.1123, 'close': 1.1124}]
    data *= 100
    time.sleep(0.2)
    for i in range(10000):
        socket.send_string(str(data))
    socket.send_string('close')
    print('done')
    time.sleep(0.1)
    socket.close()


def client():
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    # socket.setsockopt(zmq.RCVBUF, 10000)
    # socket.set_hwm(10000)
    socket.subscribe('')
    socket.connect('tcp://127.0.0.1:12345')
    count = 0
    start = time.time()
    while True:
        msg = socket.recv_string()
        if msg == 'close':
            break
        msg = eval(msg)
        count += 1
    socket.close()
    print(count)
    print(time.time() - start)


if __name__ == '__main__':
    Process(target=client).start()
    server()

