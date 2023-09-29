import pandas as pd
import numpy as np
import hashlib
import os
from dotenv import load_dotenv
import re

load_dotenv(override=True)
DEBUG = (os.getenv('DEBUG') == 'True')

def generate_leaderboard(results: pd.DataFrame, lookup: pd.DataFrame, discord_names: pd.DataFrame) -> str:
    
    # Ensure the 'id' columns in discord_names and results are treated as strings
    discord_names['id'] = discord_names['id'].astype(str)
    
    # Create a temporary lowercase version of the 'Username' for merging
    discord_names['name_lower'] = discord_names['name'].str.lower()

    # Sanitise account names to lower case for comparison.
    lookup[lookup.columns[0]] = lookup[lookup.columns[0]].str.lower()
    # Check if the third column exists, if not, create it with blank values
    if lookup.shape[1] > 2:
        lookup[lookup.columns[2]] = lookup[lookup.columns[2]].str.lower()
    else:
        lookup.insert(2, "ExpressAccountName", "")

    results['AccountName'] = results['AccountName'].str.lower()
    
    # Merge using lookup as the master table
    merged_df = lookup.merge(results,
                            left_on=lookup.columns[0],
                            right_on='AccountName',
                            how='left') \
            .rename(columns={lookup.columns[1]: 'Username', lookup.columns[2]: 'ExpressAccountName'}) \
            .drop(columns=[lookup.columns[0]])
    
    # Merge again to get the balance from ExpressAccountName
    merged_df = merged_df.merge(results,
                                left_on='ExpressAccountName',
                                right_on='AccountName',
                                how='left',
                                suffixes=('', '_express')) \
            .drop(columns=['AccountId_express','AccountName_express', 'CreatedAt_express'])

    # Drop rows with NaN values
    merged_df = merged_df.dropna(subset=['Balance'])
    
    # Convert "Balance" & "Balance_express" columns to numeric, NaN to 0.0
    merged_df['Balance'] = merged_df['Balance'].str.replace('[$,]', '', regex=True).astype(float)
    merged_df['Balance_express'] = merged_df['Balance_express'].str.replace('[$,]', '', regex=True).astype(float).fillna(0.0)

    # Calculate PnL by aggregating Balance and Balance_express
    merged_df['PnL'] = (merged_df['Balance'] + merged_df['Balance_express'] - 50000.00)

    # Sort by PnL descending
    merged_df = merged_df.sort_values('PnL', ascending=False)

    # Format the PnL as a dollar value
    merged_df['PnL'] = merged_df['PnL'].map('${:,.2f}'.format)
    
    # Create a temporary lowercase version of the 'Username' for merging
    merged_df['Username_lower'] = merged_df['Username'].str.lower()

    # Strip whitespace
    merged_df['Username_lower'] = merged_df['Username_lower'].str.strip()

    # strip #xxx references
    merged_df['Username_lower'] = merged_df['Username_lower'].str.replace(r'#\d+$', '', regex=True)

    if (DEBUG):
        print(merged_df)

    # Merge with discord_names using the lowercase versions and set ids
    merged_df = merged_df.merge(discord_names[['id', 'name', 'name_lower']], left_on='Username_lower', right_on='name_lower', how='left') \
        .drop(columns=['name', 'name_lower', 'Username_lower'])
    
    # Handle IDs and create the 'Member' column. Keep IDs as strings.
    merged_df['id'] = merged_df['id'].fillna('0')
    merged_df['id'] = merged_df['id'].apply(lambda x: f"<@{x}>" if x != '0' else None)
    merged_df['Member'] = np.where(merged_df['id'].isnull(), merged_df['Username'], merged_df['id'])

    # Update 'Member' where it is NaN with last 4 digits of 'AccountName'
    merged_df.loc[merged_df['Member'].isna(), 'Member'] = merged_df['AccountName'].str[-4:]
    
    
    # Reindex to start from 1
    merged_df.index = np.arange(1, len(merged_df) + 1)

    if merged_df.empty:
        return False
    
    # Add Emojis for discord and return
    return merged_df[['PnL', 'Member']].rename(columns={'PnL': '**PnL**', 'Member': '**Member**'}, index={1: ':first_place:', 2: ':second_place:', 3: ':third_place:'}).to_string()


def get_dataframe_hash(df: pd.DataFrame) -> str:
    # Convert the DataFrame to a binary representation and hash it
    data_hash = hashlib.sha256(pd.util.hash_pandas_object(df, index=True).values).hexdigest()

    if DEBUG:
        print('get_dataframe_hash: ', data_hash)

    return data_hash


def save_dataframe_hash(df: pd.DataFrame, filename: str) -> None:
    """
    Save the hash of a DataFrame to a specified file.

    Parameters:
    - df (pd.DataFrame): The DataFrame to hash.
    - filename (str): The name of the file to save the hash to.
    """
    data_hash = get_dataframe_hash(df)
    
    with open(filename, 'w') as f:
        f.write(data_hash)


def load_dataframe_hash(filename: str) -> str:
    """
    Load the hash of a DataFrame from a specified file.

    Parameters:
    - filename (str): The name of the file to read the hash from.

    Returns:
    - str: The hash value or an empty string if the file does not exist.
    """
    if not os.path.exists(filename):
        return ""
    
    with open(filename, 'r') as f:
        data = f.read().strip()
        if DEBUG:
            print('load_dataframe_hash: ', data)
        return data
    

def dataframe_has_changed(df: pd.DataFrame, filename: str) -> bool:
    """
    Check if the DataFrame has changed compared to a previously saved hash.

    Parameters:
    - df (pd.DataFrame): The DataFrame to check.
    - filename (str): The name of the file with the saved hash.

    Returns:
    - bool: True if the DataFrame has changed, False otherwise.
    """
    current_hash = get_dataframe_hash(df)
    saved_hash = load_dataframe_hash(filename)

    if DEBUG:
        print('dataframe_has_changed.current_hash: ', current_hash)
        print('dataframe_has_changed.saved_hash: ', saved_hash)
    
    return current_hash != saved_hash


