
import asyncio
import asyncpg as apg
import json
from cvxport import Config


async def main():
    database = Config['agent_db']
    user = Config['postgres_user']
    password = Config['postgres_pass']
    port = Config['postgres_port']

    con = await apg.connect(database=database, user=user, password=password, host='127.0.0.1', port=port)

    await con.set_type_codec('json', encoder=json.dumps, decoder=json.loads, schema='pg_catalog')
    res = await con.execute("delete from agents where name = 'mock_agent'")
    print(res)
    await con.close()


asyncio.run(main())