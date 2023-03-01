import yaml
from typing import Tuple
import asyncio
import aiohttp
import aioredis
from sys import stderr
from logging import basicConfig, INFO, getLogger
import sentry_sdk
from prometheus_client import Counter
from sentry_sdk.integrations.aiohttp import AioHttpIntegration

import statcord
from web_service import WebApp

basicConfig(stream=stderr, level=INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
log = getLogger(__name__)

bot_list_responses = Counter(
    "bot_list_responses",
    "What were the responses for bot lists",
    labelnames=["name", "status"],
    namespace="status_update"
)

bot_list_exceptions = Counter(
    "bot_list_exceptions",
    "Bot lists that throw exceptions",
    labelnames=["name"],
    namespace="status_update"
)


async def get_guild_member_count(redis) -> Tuple[int, int]:
    tr = redis.pipeline()
    guilds = tr.scard("guilds")
    members = tr.pfcount("member-count")
    await tr.execute()
    return await guilds, await members


async def post_site(client, site_name, url, json):
    auth = config["sites"].get(site_name)
    if auth:
        try:
            resp = await client.post(url=url, json=json, headers={"Authorization": auth, "Content-Type": "application/json"})
            bot_list_responses.labels(name=site_name, status=resp.status).inc()
        except aiohttp.ClientConnectorError:
            bot_list_exceptions.labels(name=site_name).inc()


async def post_bot_sites(guild_count: int, user_count: int):
    bot_id = 559426966151757824

    async with aiohttp.ClientSession(headers={"User-Agent": "NQN Not Quite Nitro Discord Bot"}) as client:
        await statcord.post(bot_id, guild_count, user_count, client, config["prometheus"], config["sites"].get("statcord"))
        await post_site(client, "top.gg", f"https://top.gg/api/bots/{bot_id}/stats", {"server_count": guild_count})
        await post_site(client, "discord.bots.gg", f"https://discord.bots.gg/api/v1/bots/{bot_id}/stats", {"guildCount": guild_count})
        await post_site(client, "discords.com", f"https://discords.com/bots/api/bot/{bot_id}", {"server_count": guild_count})
        await post_site(client, "bots.ondiscord.xyz", f"https://bots.ondiscord.xyz/bot-api/bots/{bot_id}/guilds", {"guildCount": guild_count})
        await post_site(client, "discordbotlist.com", f"https://discordbotlist.com/api/bots/{bot_id}/stats", {"guilds": guild_count, "users": user_count})


async def main():
    if config.get("sentry"):
        sentry_sdk.init(dsn=config["sentry"], integrations=[AioHttpIntegration()])
    log.info("Starting up")

    webapp = WebApp(config["web"])
    asyncio.create_task(webapp.initialise_app())

    redis = await aioredis.create_redis_pool(config["nonpersistent_redis_uri"], encoding="utf-8")

    while True:
        guilds, members = await get_guild_member_count(redis)
        await post_bot_sites(guilds, members)
        await asyncio.sleep(60)


if __name__ == "__main__":
    with open("config.yaml") as conf_file:
        config = yaml.load(conf_file, Loader=yaml.SafeLoader)

    asyncio.run(main())
