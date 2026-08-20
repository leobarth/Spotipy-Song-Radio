from SongRadio import SongRadio
import json

with open("config.json", "r") as file:
    config = json.load(file)

radio = SongRadio(**config)
radio.run()