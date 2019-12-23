
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.utils import iswrapper
from ibapi.common import TickerId
from threading import Thread
from datetime import datetime
import asyncio
import time


class SimpleClient(EWrapper, EClient):
    def __init__(self, ip, port, id):
        EWrapper.__init__(self)
        EClient.__init__(self, self)

        self.connect(ip, port, id)
        # t = Thread(target=self.run)
        # t.start()
        asyncio.run(self.my_run())

    async def my_run(self):
        print('done')
        await asyncio.sleep(0)

    async def my_actions(self):
        self.reqCurrentTime()
        await asyncio.sleep(0)

    @iswrapper
    def currentTime(self, time:int):
        t = datetime.fromtimestamp(time)
        print(f'Current time: {t}')

    @iswrapper
    def error(self, reqId:TickerId, errorCode:int, errorString:str):
        print(f'Error {errorCode}: {errorString}')


def main():
    client = SimpleClient('127.0.0.1', 7497, 0)
    client.reqCurrentTime()
    time.sleep(0.5)
    client.disconnect()


if __name__ == '__main__':
    main()