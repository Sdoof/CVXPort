
import zmq

context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect('tcp://localhost:12345')
socket.setsockopt_string(zmq.SUBSCRIBE, 'usd')
socket.setsockopt_string(zmq.SUBSCRIBE, 'eur')

while True:
    print('waiting for msg')
    msg = socket.recv_string()
    print(msg)
