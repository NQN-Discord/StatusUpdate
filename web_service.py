from aiohttp.web import Application, AppRunner, TCPSite, json_response
from logging import getLogger
from prometheus_async import aio


log = getLogger(__name__)


class WebApp:
    def __init__(self, config):
        self.config = config
        self.app = app = Application()
        app.router.add_get("/v0/", self.get_status)
        app.router.add_get("/metrics", aio.web.server_stats)

    async def initialise_app(self):
        runner = AppRunner(self.app)
        await runner.setup()
        site = TCPSite(runner, self.config["host"], self.config["port"])
        log.info(f"Started website at {self.config['host']}:{self.config['port']}")
        await site.start()

    async def get_status(self, request):
        return json_response({})
