# notes:
# introduce 

import argparse
import json
import logging


def load_config():
    """Loads config.json, falling back to config_template.json if absent.

    Args:
        None.

    Returns:
        dict: the parsed configuration.

    Raises:
        RuntimeError: if neither file exists, or if the file that does exist
            contains invalid JSON.
    """
    try:
        with open("config.json", "r") as file:
            return json.load(file)
    except FileNotFoundError:
        pass
    except json.JSONDecodeError as e:
        raise RuntimeError(f"config.json exists but is not valid JSON: {e}")

    try:
        with open("config_template.json", "r") as file:
            return json.load(file)
    except FileNotFoundError:
        raise RuntimeError(
            "Could not find 'config.json' or 'config_template.json' in this directory."
        )
    except json.JSONDecodeError as e:
        raise RuntimeError(f"config_template.json exists but is not valid JSON: {e}")


def main():
    from SongRadio import SongRadio

    config = load_config()
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


def run_with_cprofile():
    """Runs main() under cProfile.

    Note: cProfile only instruments the thread it's enabled on. Calls made
    inside SongRadio's ThreadPoolExecutor workers (the concurrent Last.fm
    lookups in filter_and_rank) will NOT be attributed here - this profile
    is only reliable for everything that still runs single-threaded
    (Spotify search/pagination, language detection, dedup, etc). Use
    run_with_yappi() for a profile that includes the worker threads.
    """
    import cProfile
    import pstats

    profiler = cProfile.Profile()
    profiler.enable()
    try:
        main()
    finally:
        profiler.disable()
        stats = pstats.Stats(profiler)
        stats.sort_stats("ncalls").print_stats(20)
        stats.sort_stats("cumulative").print_stats(20)


def run_with_yappi():
    """Runs main() under yappi, which (unlike cProfile) tracks time across threads.

    Requires `pip install yappi`. Reports wall-clock time so that time spent
    blocked on network I/O in worker threads is visible.
    """
    import yappi

    yappi.set_clock_type("wall")
    yappi.start()
    try:
        main()
    finally:
        yappi.stop()
        stats = yappi.get_func_stats()
        stats.sort("ttot", "desc")
        stats.print_all(columns={0: ("name", 80), 1: ("ncall", 10), 2: ("tsub", 8), 3: ("ttot", 8)})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Song Radio (Low-Mainstream Edition)")
    parser.add_argument("--profile", action="store_true", help="Profile with cProfile (main thread only).")
    parser.add_argument(
        "--profile-threads", action="store_true",
        help="Profile with yappi, including worker-thread time (requires `pip install yappi`).",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Show INFO-level logs (Last.fm request counts), not just warnings/alerts.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.profile_threads:
        run_with_yappi()
    elif args.profile:
        run_with_cprofile()
    else:
        main()