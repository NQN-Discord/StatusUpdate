import yaml
from typing import Tuple
import asyncio
import aiohttp
import random
from aiopg import connect
from sys import stderr
from logging import basicConfig, INFO, getLogger
import sentry_sdk


basicConfig(stream=stderr, level=INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
log = getLogger(__name__)


async def get_guild_member_count(conn) -> Tuple[int, int]:
    async with conn.cursor() as cur:
        await cur.execute("SELECT COUNT(DISTINCT guild_id) as guilds, COUNT (DISTINCT user_id) as members FROM members")
        guilds_members, = await cur.fetchall()
        return guilds_members


async def main(config):
    if config.get("sentry"):
        sentry_sdk.init(config["sentry"])
    log.info("Starting up")
    gateway_url = config["gateway_url"]
    async with connect(config["postgres_uri"]) as conn:
        async with aiohttp.ClientSession() as session:
            while True:
                guilds, members = await get_guild_member_count(conn)
                counts = [f"{guilds} servers", f"{members} members"]
                random.shuffle(counts)
                message = f"{counts[0]} and {counts[1]}"
                log.debug(f"Sending {message!r}")
                try:
                    await session.put(f"{gateway_url}/status", json={"status": message})
                except aiohttp.ClientConnectorError:
                    pass

                await asyncio.sleep(60)


if __name__ == "__main__":
    with open("config.yaml") as conf_file:
        config = yaml.load(conf_file, Loader=yaml.SafeLoader)

    asyncio.run(main(config))
