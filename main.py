import cProfile
import pstats

def main():
    from SongRadio import SongRadio
    import json

    try:
        with open("config.json", "r") as file:
            config = json.load(file)
    except FileNotFoundError:
        with open("config_template.json", "r") as file:
            config = json.load(file)
    except:
        raise RuntimeError("Could not resolve config from file. Please ensure either 'config.json' or 'config_template.json' exists in this directory.")

    radio = SongRadio(**config)

    print("Resolving seed tracks...")
    seed_track_ids = radio.resolve_seed_track_ids()
    if not seed_track_ids:
        raise RuntimeError("No seed tracks found - aborting.")

    print("\nLoading artist data and genres...")
    seed_artist_ids, genre_counter = radio.get_seed_artist_ids_and_genres(seed_track_ids)

    print("\n--- Detected Genres ---")
    for genre, count in genre_counter.most_common(10):
        print(f"  {genre}  (x{count})")

    print("\nBuilding candidate pool...")
    candidates = radio.build_candidate_pool(genre_counter)
    print(f"{len(candidates)} candidates found.\n")

    print("Selecting tracks...")
    radio.filter_and_rank(candidates)
    radio.print_results()

    answer = input("\nSave as a playlist in your library? [Y/n] ").strip().lower()
    if answer in ("", "y", "yes"):
        radio.save_as_playlist()
    else:
        print("Playlist will not be saved.")

if __name__ == "__main__":
    profiler = cProfile.Profile()
    profiler.enable()
    main()
    profiler.disable()

    stats = pstats.Stats(profiler)
    stats.sort_stats("ncalls").print_stats(20)      # most-called functions
    stats.sort_stats("cumulative").print_stats(20)  # where time is actually spent