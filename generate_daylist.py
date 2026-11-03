"""CLI entry point for generating and refreshing the persistent Daylist."""

import argparse
import logging
import os

from config_utils import keyword_parameters, load_config as _load_config

logger = logging.getLogger("song_radio.daylist.cli")

DAYLIST_KEYS = {
    "daylist_num_seeds",
    "daylist_familiarity_penalty",
    "daylist_seed_pool_size",
    "timezone_name",
    "state_file",
}


def load_config(name="daylist_config"):
    """Load and validate the Daylist JSON configuration."""
    from Daylist import Daylist
    from SongRadio import SongRadio

    allowed = keyword_parameters(SongRadio) | DAYLIST_KEYS
    return _load_config(
        name,
        allowed_keys=allowed,
        required_keys={"client_id", "client_secret", "redirect_uri", "lastfm_api_key"},
    )


def _split_config(config):
    """Split Daylist-specific settings from SongRadio constructor arguments."""
    daylist_kwargs = {key: value for key, value in config.items() if key in DAYLIST_KEYS}
    radio_keys = keyword_parameters(__import__("SongRadio").SongRadio)
    radio_kwargs = {key: value for key, value in config.items() if key in radio_keys}
    radio_kwargs.setdefault("seed_queries", {"Placeholder": "Placeholder"})
    return daylist_kwargs, radio_kwargs


def main():
    """Run Daylist and always close its Spotify/Last.fm resources."""
    from Daylist import Daylist

    config = load_config()
    daylist_kwargs, radio_kwargs = _split_config(config)
    daylist = Daylist(radio_kwargs=radio_kwargs, **daylist_kwargs)
    try:
        result = daylist.run()
        logger.info("Daylist updated: https://open.spotify.com/playlist/%s", result["playlist_id"])
    finally:
        daylist.close()


def run_with_cprofile():
    """Run main under cProfile and print the most expensive functions."""
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
    """Print a bounded summary of yappi function statistics."""
    print(f"\n--- {label} (top {limit}) ---")
    print(f"{'ncall':>8}  {'ttot(s)':>9}  {'tsub(s)':>9}  name")
    for index, stat in enumerate(stats):
        if index >= limit:
            break
        print(f"{stat.ncall:>8}  {stat.ttot:>9.3f}  {stat.tsub:>9.3f}  {stat.full_name}")


def run_with_yappi():
    """Run main under yappi, including time spent in worker threads."""
    import yappi

    yappi.set_clock_type("wall")
    yappi.start()
    try:
        main()
    finally:
        yappi.stop()
        function_stats = yappi.get_func_stats()
        function_stats.sort("ttot", "desc")

        project_files = {"generate_daylist.py", "Daylist.py", "SongRadio.py", "StreamingHistory.py"}
        project_stats = [
            stat for stat in function_stats
            if stat.module and os.path.basename(stat.module) in project_files
        ]
        _print_func_stats(project_stats, 25, "Project code")
        _print_func_stats(list(function_stats), 15, "All code")

        print("\n--- Per-thread summary ---")
        thread_stats = yappi.get_thread_stats()
        thread_stats.sort("ttot", "desc")
        thread_stats.print_all()
        yappi.clear_stats()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate or refresh the SongRadio Daylist.")
    parser.add_argument("--profile", action="store_true", help="Profile with cProfile.")
    parser.add_argument("--profile-threads", action="store_true", help="Profile with yappi, including worker threads.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable INFO-level logging.")
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