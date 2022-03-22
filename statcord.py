import time
from collections import Counter
import psutil


async def post(bot_id: int, guild_count: int, user_count: int, client, prom_url: str, secret_key: str):
    if not secret_key:
        return
    mem = psutil.virtual_memory()
    resp = await client.get(
        f"{prom_url}/api/v1/query",
        params={
            "query": '60*rate(commands_command_latency_count{command=~"[^_].*"}[1m])',
            "time": str(time.time())
        })
    prom_data = await resp.json()
    commands = Counter({cmd["metric"]["command"]: int(float(cmd["value"][1])) for cmd in prom_data["data"]["result"] if len(cmd["metric"]["command"]) > 2})

    await client.post(
        url=f"https://api.statcord.com/v3/stats",
        json={
            "id": str(bot_id),
            "key": secret_key,
            "servers": str(guild_count),
            "users": str(user_count),
            "active": [],
            "commands": str(sum(commands.values())),
            "popular": [{"name": name, "count": count} for name, count in commands.most_common(20)],
            "memactive": str(mem.used),
            "memload": str(mem.percent),
            "cpuload": str(psutil.cpu_percent()),
            "bandwidth": "0",
        },
        headers={"Content-Type": "application/json"}
    )
