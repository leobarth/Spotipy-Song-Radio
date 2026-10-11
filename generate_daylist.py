"""
generate_daylist.py
===================

Entry point for the Daylist pipeline: selects seed artists from the user's
streaming history based on the current time of day and day of week, runs
the SongRadio recommendation pipeline on those seeds, applies a familiarity
penalty to push results toward discovery, and atomically replaces the
content of a single persistent Spotify playlist.

No "save as playlist?" prompt - the Daylist is always saved, since its
entire purpose is to refresh a persistent playlist on every run.

Config
------
Reads daylist_config.json (falls back to daylist_config_template.json).
This file must contain all required SongRadio fields plus these Daylist-
specific keys:

  (no playlist_id needed) - the Daylist creates and manages its own
      playlist automatically. The ID is saved in daylist_state.json and
      reused on every subsequent run. If the playlist is deleted, a new
      one is created automatically on the next run.
  daylist_num_seeds (int, default 2): Artists sampled as seeds per run.
  daylist_familiarity_penalty (float, default 0.3): 0 = no penalty, 1 =
      fully zeroes similarity for the most familiar artists.
  daylist_seed_pool_size (int, default 20): How many top time-weighted
      artists form the pool from which seeds are sampled.
  utc_offset_hours (int, default 0): UTC offset for local-time bucketing
      (e.g. 1 for CET, 2 for CEST).

Streaming history must have been loaded at least once beforehand via:
  python generate_song_radio.py --streaming-history /path/to/export

Profiling
---------
--profile        cProfile (main thread only)
--profile-threads  yappi (all threads, wall-clock time)
-v / --verbose   INFO-level logging
"""

import argparse
import json
import logging
import os


def load_config(name="daylist_config"):
    """Loads <name>.json, falling back to <name>_template.json if absent.

    Args:
        name: Config file stem (without extension).

    Returns:
        dict: the parsed configuration.

    Raises:
        RuntimeError: if neither file exists or the present file has
            invalid JSON.
    """
    for filename in (f"{name}.json", f"{name}_template.json"):
        try:
            with open(filename, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            continue
        except json.JSONDecodeError as e:
            raise RuntimeError(f"{filename} exists but is not valid JSON: {e}")
    raise RuntimeError(
        f"Could not find '{name}.json' or '{name}_template.json' in this directory."
    )


def _split_config(config):
    """Separates Daylist-specific keys from SongRadio kwargs.

    Args:
        config: Full merged config dict from daylist_config.json.

    Returns:
        (daylist_kwargs, radio_kwargs) tuple.
    """
    daylist_keys = {
        "daylist_num_seeds",
        "daylist_familiarity_penalty",
        "daylist_seed_pool_size",
        "utc_offset_hours",
    }
    daylist_kwargs = {k: v for k, v in config.items() if k in daylist_keys}
    radio_kwargs = {k: v for k, v in config.items() if k not in daylist_keys}
    # SongRadio requires seed_queries at construction time even though
    # Daylist overrides it at runtime - supply a minimal placeholder
    # if the config file doesn't have one (it shouldn't need to).
    radio_kwargs.setdefault("seed_queries", {"Placeholder": "Placeholder"})
    return daylist_kwargs, radio_kwargs


def main():
    from Daylist import Daylist

    config = load_config("daylist_config")
    daylist_kwargs, radio_kwargs = _split_config(config)

    daylist = Daylist(radio_kwargs=radio_kwargs, **daylist_kwargs)

    try:
        daylist.run()
    finally:
        daylist.close()


def run_with_cprofile():
    """Runs main() under cProfile (main thread only).

    See generate_song_radio.py for the same caveat: worker-thread time
    (Last.fm concurrent lookups) is not attributed here.
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
    """Runs main() under yappi (all threads, wall-clock time).

    Requires `pip install yappi`.
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

        project_files = {
            "generate_daylist.py", "Daylist.py",
            "SongRadio.py", "StreamingHistory.py",
        }
        project_stats = [s for s in func_stats if os.path.basename(s.module) in project_files]
        _print_func_stats(project_stats, limit=25, label="Project code")
        _print_func_stats(list(func_stats), limit=15, label="All code")

        print("\n--- Per-thread summary ---")
        thread_stats = yappi.get_thread_stats()
        thread_stats.sort("ttot", "desc")
        thread_stats.print_all()

        yappi.clear_stats()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daylist — time-aware SongRadio playlist")
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