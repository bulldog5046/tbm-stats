import os
from tbm_stats import tbm_stats
import pandas as pd
import utils
from tbm_stats import tbm_stats
from discord_bot import DiscordBot
from dotenv import load_dotenv
import time

load_dotenv(override=True)
DEBUG = (os.getenv('DEBUG') == 'True')

if DEBUG:
    os.environ["GOOGLE_SHEET"] = os.getenv('DEBUG_GOOGLE_SHEET')

tbm = tbm_stats()

if DEBUG:
    #results = pd.read_json(os.getenv('DEBUG_TS_DATA')) # use cached stats
    results = tbm.get_results()
else:
    results = tbm.get_results()

if (not utils.dataframe_has_changed(results, os.getenv('HASH_FILE')) and not DEBUG):
    print('Data is the same. Exiting early.')
    exit()

# Save hash of the new dateframe
print('Saving Dataframe Hash: ', utils.get_dataframe_hash(results))
utils.save_dataframe_hash(results, os.getenv('HASH_FILE'))

lookup = pd.read_csv(os.getenv('GOOGLE_SHEET'))

print('Starting Discord Bot..')
discord = DiscordBot()
discord.run_bot()

print('Waiting for Discord Bot to be ready..')
while(not discord.is_ready()):
    time.sleep(1)

members = discord.get_channel_members()

data = [{
    'id': member.id,
    'name': member.name,
    'global_name': getattr(member, 'global_name', None),  # Using getattr in case some attributes might be missing
    'nick': member.nick,
    'guild_id': member.guild.id,
    'guild_name': member.guild.name,
} for member in members]

# Convert the list of dictionaries into a DataFrame
members_df = pd.DataFrame(data)

leaderboard = utils.generate_leaderboard(results, lookup, members_df)

# debug
if (DEBUG):
    print(members_df)
    print(leaderboard)
    discord.stop_bot()
    exit()
# /debug

if leaderboard == False:
    discord.stop_bot()
    print('Something went wrong, leaderboard is empty.')
    exit()

print(leaderboard)

print('Sending Message to Discord..')
discord.send_message(leaderboard)

time.sleep(25) # wait for message to send before killing.

discord.stop_bot()