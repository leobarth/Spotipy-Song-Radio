# notes:
# exploration factor for candidate building
# ensure the max candidate limit is reached
# all in all fix candidate building

import argparse
import json
import logging
import os


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

    try:
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

        print("\n" + radio.lastfm_health_summary())

        answer = input("\nSave as a playlist in your library? [Y/n] ").strip().lower()
        if answer in ("", "y", "yes"):
            radio.save_as_playlist()
        else:
            print("Playlist will not be saved.")
    finally:
        radio.close()


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


def _print_func_stats(stats, limit, label):
    """Prints a short, fixed-width table for a yappi func-stats iterable.

    yappi's own stats.print_all() dumps every profiled function - typically
    thousands of stdlib/library entries - which is unreadable in a terminal.
    This prints at most `limit` rows instead.

    Args:
        stats: Iterable of yappi YFuncStat entries, already sorted.
        limit: Maximum number of rows to print.
        label: Section heading.

    Returns:
        None.
    """
    print(f"\n--- {label} (top {limit}) ---")
    print(f"{'ncall':>8}  {'ttot(s)':>9}  {'tsub(s)':>9}  name")
    shown = 0
    for stat in stats:
        if shown >= limit:
            break
        print(f"{stat.ncall:>8}  {stat.ttot:>9.3f}  {stat.tsub:>9.3f}  {stat.full_name}")
        shown += 1


def run_with_yappi():
    """Runs main() under yappi, which (unlike cProfile) tracks time across threads.

    Requires `pip install yappi`. Reports wall-clock time so that time spent
    blocked on network I/O in worker threads is visible. Output is
    deliberately trimmed to three short sections instead of yappi's default
    full dump (which lists every stdlib/library function and floods the
    terminal):
      1. Project code only (this file + SongRadio.py), sorted by total time.
      2. Top 15 functions overall (including libraries), for context.
      3. A one-row-per-thread summary, showing how much wall time each
         worker thread actually spent - the most direct evidence of whether
         concurrency is being used effectively.
    """
    import yappi

    yappi.set_clock_type("wall")
    yappi.start()
    try:
        main()
    finally:
        yappi.stop()

        func_stats = yappi.get_func_stats()
        func_stats.sort("ttot", "desc")

        project_stats = [s for s in func_stats if os.path.basename(s.module) in {"main.py", "SongRadio.py"}]
        _print_func_stats(project_stats, limit=25, label="Project code")
        _print_func_stats(list(func_stats), limit=15, label="All code")

        print("\n--- Per-thread summary ---")
        thread_stats = yappi.get_thread_stats()
        thread_stats.sort("ttot", "desc")
        thread_stats.print_all()

        yappi.clear_stats()


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