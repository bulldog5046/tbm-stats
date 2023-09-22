import os
import threading
import discord
import time
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()

class DiscordBot(commands.Bot):
    def __init__(self):
        self.channel = None
        intents = discord.Intents.default()
        intents.members = True
        super().__init__(command_prefix="!", intents=intents)

    async def on_ready(self):
        self.channel = self.get_channel(int(os.getenv('DISCORD_CHANNEL')))
        print(f"Bot {self.user.display_name} is connected to server. Channel is {self.channel}")

    def send_message(self, message):
        self.channel = self.get_channel(int(os.getenv('DISCORD_CHANNEL')))
        self.loop.create_task(self.channel.send(message))

    def get_channel_members(self):
        return self.channel.members
    
    def is_channel_available(self):
        return self.channel is not None

    def run_bot(self):
        t = threading.Thread(target=self.run, args=[os.getenv('DISCORD_TOKEN')])
        t.start()

    def stop_bot(self):
        self.loop.create_task(self.close())


if __name__ == "__main__":
    instance = DiscordBot()
    
    instance.run_bot()

    print('hello')

    time.sleep(10)

    instance.send_message('does it work now?')

    members = instance.get_channel_members()


    print(members)
    time.sleep(10)

    instance.stop_bot()

    instance.get
 