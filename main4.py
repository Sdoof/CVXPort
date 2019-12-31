
import asyncpg as apg
import asyncio
from datetime import datetime
from pytz import timezone
import time


async def main():
    con = await apg.connect(database='bar_data', user='postgres', password='key')
    # sql = f'''
    #     create table test(
    #         datetime timestamp not null,
    #         open double precision not null,
    #         close double precision not null,
    #         primary key(datetime)
    #     )
    # '''
    sql = f'insert into test(datetime, open, close) values($1, $2, $3)'
    start = time.time()
    for _ in range(10000):
        await con.execute(sql, datetime.now(), 1, 2)
        time.sleep(1e-5)
    print(time.time() - start)
    await con.close()

asyncio.run(main())
