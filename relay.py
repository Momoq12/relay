import asyncio
import os
import discord
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

READER_TOKEN = os.getenv("READER_TOKEN")
BOT_TOKEN    = os.getenv("BOT_TOKEN")

if not READER_TOKEN or not BOT_TOKEN:
    raise ValueError("Missing tokens in .env file")

# map: source_channel_id -> target_channel_id
ROUTES = {
    REPLACE_WITH_CHANNEL_ID: REPLACE_WITH_CHANNEL_ID, # Read channel 1:Write channel 1
    REPLACE_WITH_CHANNEL_ID: REPLACE_WITH_CHANNEL_ID, # Read channel 2:Write channel 2
    REPLACE_WITH_CHANNEL_ID: REPLACE_WITH_CHANNEL_ID, # Read channel 3:Write channel 3

    #Add as many channels as you want 
    #The reader account must be in the server whose messages you want to relay
    #The writer account must be in the server where the relays messages will be displayed
    
}
REVERSE_ROUTES = {v: k for k, v in ROUTES.items()}

COMMAND_PREFIX = "."

reader = discord.Client()
bot    = discord.Client()

_relay_ready = False
relay_queue  = asyncio.Queue()

@reader.event
async def on_ready():
    global _relay_ready
    print(f"[reader] {reader.user} ({reader.user.id})")
    for src_id in ROUTES.keys():
        channel = reader.get_channel(src_id)
        if channel:
            print(f"[reader] watching #{channel.name} in {channel.guild.name}")
        else:
            print(f"[reader] source {src_id} NOT in cache")
    _relay_ready = True

@reader.event
async def on_message(message):
    if not _relay_ready or message.guild is None:
        return
    if message.author.id == reader.user.id:
        return

    # ── relay handling (Source -> Target) ──────────────────────
    if message.channel.id in ROUTES:
        if not message.content.strip() and not message.attachments:
            return
        
        header = f"**{message.author.display_name}** (`{message.author.name}` | `{message.author.id}`)"
        body = message.content if message.content else "[attachment]"
        payload = f"{header}\n{body}"
        if len(payload) > 2000:
            payload = payload[:1997] + "..."
            
        await relay_queue.put((ROUTES[message.channel.id], payload))

# ── worker task for the bot to process the queue ───────────────
async def queue_worker():
    await bot.wait_until_ready()
    while not bot.is_closed():
        target_id, payload = await relay_queue.get()
        target = bot.get_channel(target_id)
        if target is None:
            print(f"[relay] bot target {target_id} not cached")
        else:
            try:
                await target.send(payload)
                print(f"[relay] bot sent to {target_id}")
            except discord.HTTPException as e:
                print(f"[relay] bot send failed: {e.status}")
        relay_queue.task_done()

@bot.event
async def on_ready():
    print(f"[bot] {bot.user} ({bot.user.id})")
    for target_id in REVERSE_ROUTES.keys():
        ch = bot.get_channel(target_id)
        print(f"[bot] target {target_id} cached: {ch is not None}")

@bot.event
async def on_message(message):
    if message.author.bot:
        return
    
    # ── command handling (Target -> Source) ────────────────────
    if message.channel.id in REVERSE_ROUTES and message.content.startswith(COMMAND_PREFIX):
        parts = message.content[len(COMMAND_PREFIX):].split(maxsplit=1)
        if len(parts) == 2 and parts[0] == "say":
            source_channel_id = REVERSE_ROUTES[message.channel.id]
            
            # Resolve the channel through the reader's guild cache
            target = None
            for guild in reader.guilds:
                target = guild.get_channel(source_channel_id)
                if target:
                    break
                    
            if target:
                try:
                    await target.send(parts[1])
                    await message.channel.send("`[sent to source]`")
                except Exception as e:
                    await message.channel.send(f"`[error: {e}]`")
            else:
                await message.channel.send("`[reader cannot see source channel]`")

async def main():
    await asyncio.gather(
        reader.start(READER_TOKEN),
        bot.start(BOT_TOKEN),
        queue_worker()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
