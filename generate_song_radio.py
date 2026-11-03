"""CLI entry point for generating SongRadio recommendations."""

import argparse
import logging
import os

from config_utils import keyword_parameters, load_config as _load_config

logger = logging.getLogger("song_radio.cli")


def load_config(name="config"):
    """Load and validate the SongRadio configuration."""
    from SongRadio import SongRadio

    return _load_config(
        name,
        allowed_keys=keyword_parameters(SongRadio) | {"timezone_name"},
        required_keys={
            "client_id",
            "client_secret",
            "redirect_uri",
            "lastfm_api_key",
            "seed_queries",
        },
    )


def main():
    """Run SongRadio and optionally save its recommendations as a playlist."""
    from SongRadio import SongRadio

    config = load_config()
    seed_queries = config.get("seed_queries")
    if not isinstance(seed_queries, dict) or not seed_queries:
        raise RuntimeError("config.json must define at least one seed artist and track.")

    radio_config = {key: value for key, value in config.items() if key != "timezone_name"}
    radio = SongRadio(**radio_config)
    try:
        result = radio.run_pipeline()

        print("\n--- Detected Genres ---")
        for genre, count in result["genres"].most_common(10):
            print(f"  {genre} (x{count})")

        print(f"\n{len(result['candidates'])} candidates found.")
        radio.print_results()
        print("\n" + result["lastfm_health"])

        answer = input("\nSave as a playlist in your library? [Y/n] ").strip().casefold()
        if answer in ("", "y", "yes"):
            playlist_id = radio.save_as_playlist()
            print(f"Playlist saved: https://open.spotify.com/playlist/{playlist_id}")
        else:
            print("Playlist will not be saved.")
    finally:
        radio.close()


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

        project_files = {"generate_song_radio.py", "SongRadio.py", "StreamingHistory.py"}
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
    parser = argparse.ArgumentParser(description="Generate SongRadio recommendations.")
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