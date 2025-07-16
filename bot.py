import json
from interactions import Client, Intents, listen, slash_command, SlashContext
config = json.load(open("../config.json"))

bot = Client(intents=Intents.DEFAULT | Intents.MESSAGE_CONTENT)

@slash_command(name="my_command", description="My first command :)")
async def my_command_function(ctx: SlashContext):
    await ctx.send("Hello World")

bot.start(config["token"])