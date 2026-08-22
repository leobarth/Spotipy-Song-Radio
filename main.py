"""
ideas:
    
"""

from SongRadio import SongRadio
import json

try:
    with open("config.json", "r") as file:
        config = json.load(file)
except FileNotFoundError:
    with open("config_template.json", "r") as file:
        config = json.load(file)

radio = SongRadio(**config)

print("Loese Seed-Tracks auf...")
seed_track_ids = radio.resolve_seed_track_ids()
if not seed_track_ids:
    raise KeyError("Keine Seed-Tracks gefunden - Abbruch.")

print("\nLade Kuenstlerdaten und Genres...")
seed_artist_ids, genre_counter = radio.get_seed_artist_ids_and_genres(seed_track_ids)

print("\n--- Erkannte Genres ---")
for genre, count in genre_counter.most_common(10):
    print(f"  {genre}  (x{count})")

print("\nBaue Kandidaten-Pool...")
candidates = radio.build_candidate_pool(genre_counter)
print(f"{len(candidates)} Kandidaten gefunden.\n")

print("Waehle Tracks aus...")
radio.filter_and_rank(candidates)
radio.print_results()

answer = input("\nAls Playlist in die Bibliothek speichern? [Y/n] ").strip().lower()
if answer in ("", "y", "yes"):
    radio.save_as_playlist()
else:
    print("Playlist wird nicht gespeichert.")