import os
from tbm_stats import tbm_stats
import pandas as pd
import utils
from tbm_stats import tbm_stats
from discord_bot import DiscordBot
from dotenv import load_dotenv
from datetime import datetime, timedelta
import numpy as np
import time

load_dotenv(override=True)
DEBUG = (os.getenv('DEBUG') == 'True')

if DEBUG:
    os.environ["GOOGLE_SHEET"] = os.getenv('DEBUG_GOOGLE_SHEET')

tbm = tbm_stats()

if DEBUG:
    #results = pd.read_json(os.getenv('DEBUG_TS_DATA')) # use cached stats
    #results = tbm.get_results()

    # create spoof dataframe
    src = pd.read_csv(os.getenv('GOOGLE_SHEET'), header=None)
    # Generate random timestamps
    start_date = datetime(2023, 8, 1)
    end_date = datetime(2023, 9, 30)
    date_range = [start_date + timedelta(seconds=np.random.randint(0, int((end_date-start_date).total_seconds()))) for _ in range(len(src))]
    # Generate random balances
    balances = ["${:,.2f}".format(np.random.uniform(47000, 50000)) for _ in range(len(src))]
    results = pd.DataFrame({
        "CreatedAt": date_range,
        "AccountId": 123123,
        "AccountName": src[src.columns[0]],
        "Balance": balances
    })


else:
    results = tbm.get_results()

# Normalize the dataframe for hash generation/comparison
results = results.sort_values('AccountId').reset_index(drop=True)
print('=============Results DF=============')
print(results)

if (not utils.dataframe_has_changed(results, os.getenv('HASH_FILE')) and not DEBUG):
    print('Data is the same. Exiting early.')
    exit()

# Save hash of the new dateframe
print('Saving Dataframe Hash: ', utils.get_dataframe_hash(results))
utils.save_dataframe_hash(results, os.getenv('HASH_FILE'))

if os.getenv('SHEET_TYPE') == "2":
    data = pd.read_csv(os.getenv('GOOGLE_SHEET'))
    data['Accounts'] = data['Accounts'].apply(eval)
    lookup = data.explode('Accounts').reset_index().drop(columns=['index'])
    lookup_type = 2
else:
    lookup = pd.read_csv(os.getenv('GOOGLE_SHEET'), header=None)
    lookup_type = 1

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

leaderboard = utils.generate_leaderboard(results, lookup, members_df, lookup_format=lookup_type)

if leaderboard == False:
    discord.stop_bot()
    print('Something went wrong, leaderboard is empty.')
    exit()

header_string = f":trophy: ***{datetime.now().strftime('%B')} Challenge*** :trophy:\n"

message = header_string + leaderboard.replace(': ', ':         ', 3).replace('second_place:', 'second_place: ', 1)

print(message)

# debug
if (DEBUG):
    print(members_df)
    print(leaderboard)
    print(message)
    discord.stop_bot()
    exit()
# /debug

print('Sending Message to Discord..')
discord.send_message(message)

time.sleep(25) # wait for message to send before killing.

discord.stop_bot()