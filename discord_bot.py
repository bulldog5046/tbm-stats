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
        max_len = 2000
        lines = message.split("\n")
        chunks = []
        current_chunk = ""

        for line in lines:
            if len(current_chunk) + len(line) + 1 > max_len:  # +1 for the newline character
                chunks.append(current_chunk)
                current_chunk = line + "\n"
            else:
                current_chunk += line + "\n"

        if current_chunk:
            chunks.append(current_chunk)

        self.channel = self.get_channel(int(os.getenv('DISCORD_CHANNEL')))

        self._send_chunks_sequentially(chunks)

    def _send_single_chunk(self, chunk, remaining_chunks):
        def callback(task):
            if remaining_chunks:
                next_chunk = remaining_chunks.pop(0)
                self._send_single_chunk(next_chunk, remaining_chunks)

        task = self.loop.create_task(self.channel.send(chunk))
        task.add_done_callback(callback)

    def _send_chunks_sequentially(self, chunks):
        if chunks:
            first_chunk = chunks.pop(0)
            self._send_single_chunk(first_chunk, chunks)


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
 