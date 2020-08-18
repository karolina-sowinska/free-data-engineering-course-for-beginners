import sqlalchemy
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3


DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "lifewithkarcia"
TOKEN = "BQCzxQpW4-JYstKcMkSlRDM5yT8FLoQh3WQ7EcR9ve90cZa4rLe9ETOGfUjs9Ls6Xtv-1rZPgTJ9uSxKTNVPMFyZoYcNtP5kK0VE478B1JElangA6JQpe0qsOZv_W4FzyEJEIsiUjBHyDzNKY_DmGO0m212GTA"

if __name__ == "__main__":

    # Extract part of the ETL process
    
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)

    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10]) 

    song_dict = {
        "song_name" : song_names,
        "artist_name": artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])

    print(song_df)

        
    # Transform
    # ...
    
    # Load
    # ...
    
    # Job scheduling 
    # ...
