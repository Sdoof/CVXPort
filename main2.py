
from aiohttp import web
import asyncio


async def hello(request):
    return web.Response(text="Hello, Albert", )

app = web.Application()
app.add_routes([web.get('/hello', hello)])


async def main():
    # noinspection PyProtectedMember
    await asyncio.gather(web.run_app(app))

asyncio.run(main())