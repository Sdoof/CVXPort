
import zmq


context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect('tcp://127.0.0.1:6006')
socket.send_string('FX:EURUSD,FX:GBPUSD,FX:AUDUSD,FX:USDJPY')
msg = socket.recv_json()
print(msg)