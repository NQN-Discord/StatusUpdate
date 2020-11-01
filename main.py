import yaml
from typing import Tuple
import asyncio
import aiohttp
import random
from aiopg import connect
from sys import stderr
from logging import basicConfig, INFO, DEBUG, getLogger
import sentry_sdk

from rabbit_sender import StatusUpdateRabbit


basicConfig(stream=stderr, level=INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
log = getLogger(__name__)


async def get_guild_member_count(conn) -> Tuple[int, int]:
    async with conn.cursor() as cur:
        await cur.execute("SELECT COUNT(DISTINCT guild_id) as guilds, COUNT (DISTINCT user_id) as members FROM members")
        guilds_members, = await cur.fetchall()
        return guilds_members


async def post_bot_sites(guild_count: int, user_count: int):
    bot_id = 559426966151757824  # bot.user.id

    async with aiohttp.ClientSession(headers={"User-Agent": "NQN Not Quite Nitro Discord Bot"}) as client:
        dbl = config["sites"].get("dbl")
        if dbl:
            await client.post(
                url=f"https://top.gg/api/bots/{bot_id}/stats",
                json={
                    "server_count": guild_count
                },
                headers={"Authorization": dbl, "Content-Type": "application/json"}
            )
        dbgg = config["sites"].get("dbgg")
        if dbgg:
            await client.post(
                url=f"https://discord.bots.gg/api/v1/bots/{bot_id}/stats",
                json={
                    "guildCount": guild_count
                },
                headers={"Authorization": dbgg, "Content-Type": "application/json"}
            )
        boats = config["sites"].get("boats")
        if boats:
            await client.post(
                url=f"https://discord.boats/api/bot/{bot_id}",
                json={
                    "server_count": guild_count,
                },
                headers={"Authorization": boats, "Content-Type": "application/json"}
            )
        bfd = config["sites"].get("bfd")
        if bfd:
            await client.post(
                url=f"https://botsfordiscord.com/api/bot/{bot_id}",
                json={
                    "server_count": guild_count,
                },
                headers={"Authorization": bfd, "Content-Type": "application/json"}
            )
        bls = config["sites"].get("bls")
        if bls:
            await client.post(
                url=f"https://api.botlist.space/v1/bots/{bot_id}",
                json={
                    "server_count": guild_count,
                },
                headers={"Authorization": bls, "Content-Type": "application/json"}
            )
        bod = config["sites"].get("bod")
        if bod:
            await client.post(
                url=f"https://bots.ondiscord.xyz/bot-api/bots/{bot_id}/guilds",
                json={
                    "guildCount": guild_count,
                },
                headers={"Authorization": bod, "Content-Type": "application/json"}
            )
        dblc = config["sites"].get("dblc")
        if dblc:
            await client.post(
                url=f"https://discordbotlist.com/api/bots/{bot_id}/stats",
                json={
                    "guilds": guild_count,
                    "users": user_count
                },
                headers={"Authorization": f"Bot {dblc}", "Content-Type": "application/json"}
            )
        extreme = config["sites"].get("extreme")
        if extreme:
            await client.post(
                url=f"https://api.discordextremelist.xyz/v2/bot/{bot_id}/stats",
                json={
                    "guildCount": guild_count,
                },
                headers={"Authorization": extreme, "Content-Type": "application/json"}
            )


async def main(config):
    if config.get("sentry"):
        sentry_sdk.init(config["sentry"])
    log.info("Starting up")

    rabbit = StatusUpdateRabbit(config["rabbit_uri"])
    await rabbit.connect()

    async with connect(config["postgres_uri"]) as conn:
        while True:
            guilds, members = await get_guild_member_count(conn)
            counts = [f"{guilds} servers", f"{members} members"]
            random.shuffle(counts)
            message = f"{counts[0]} and {counts[1]}"
            log.debug(f"Sending {message!r}")
            try:
                await rabbit.send_status(message)
                await post_bot_sites(guilds, members)
            except aiohttp.ClientConnectorError:
                pass

            await asyncio.sleep(60)


if __name__ == "__main__":
    with open("config.yaml") as conf_file:
        config = yaml.load(conf_file, Loader=yaml.SafeLoader)

    asyncio.run(main(config))
