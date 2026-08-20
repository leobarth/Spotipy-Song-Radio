"""
ideas:
    filter songs in results to avoid duplicates
    randomize offset values
    find different names for new playlists
"""

from SongRadio import SongRadio
import json

with open("config.json", "r") as file:
    config = json.load(file)

radio = SongRadio(**config)
radio.run()