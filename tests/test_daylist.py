import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from datetime import datetime, timezone

from Daylist import Daylist, _current_slot_and_weekday


class FakeHistoryRadio:
    def get_streaming_history_artists(self):
        return [
            ("artist-a", "Artist A", 10.0),
            ("artist-b", "Artist B", 5.0),
        ]

    def get_top_tracks_by_artist(self):
        return {
            "artist-a": ("Artist A", "Track A"),
            "artist-b": ("Artist B", "Track B"),
        }

    def get_time_bucket_scores(self, slot, weekday):
        return {
            "artist-a": {"slot_score": 10.0, "weekday_score": 2.0, "joint_score": 1.0},
            "artist-b": {"slot_score": 2.0, "weekday_score": 10.0, "joint_score": 1.0},
        }


class DaylistTests(unittest.TestCase):
    def test_current_slot_handles_dst_transition(self):
        result = _current_slot_and_weekday(
            "Europe/Berlin",
            now=datetime(2026, 3, 29, 4, 30, tzinfo=timezone.utc),
        )
        self.assertEqual(result, ("morning", "sunday"))

    def test_seed_selection_uses_original_names_and_tracks(self):
        daylist = Daylist.__new__(Daylist)
        daylist.radio = FakeHistoryRadio()
        daylist.timezone_name = "Europe/Berlin"
        daylist.daylist_seed_pool_size = 10
        daylist.daylist_num_seeds = 2

        with patch("Daylist._current_slot_and_weekday", return_value=("evening", "friday")):
            seeds = daylist._select_seeds_for_now()

        self.assertEqual(len(seeds), 2)
        self.assertEqual({seed[1] for seed in seeds}, {"Artist A", "Artist B"})
        self.assertEqual({seed[2] for seed in seeds}, {"Track A", "Track B"})

    def test_familiarity_percentiles_handle_ties(self):
        picks = [
            {"track": {"artist": "Artist A"}, "similarity": 1.0},
            {"track": {"artist": "Artist B"}, "similarity": 1.0},
            {"track": {"artist": "Artist C"}, "similarity": 1.0},
        ]
        radio = SimpleNamespace(
            picks=picks,
            get_familiarity_scores=lambda: {"artist a": 1.0, "artist b": 1.0, "artist c": 9.0},
            _track_artist_names=lambda track: [track["artist"]],
        )
        daylist = Daylist.__new__(Daylist)
        daylist.radio = radio
        daylist.daylist_familiarity_penalty = 0.4
        daylist._familiarity_penalty_stats = {}

        daylist._apply_familiarity_penalty()

        self.assertAlmostEqual(daylist._familiarity_penalty_stats["Artist A"]["percentile"], 0.25)
        self.assertAlmostEqual(daylist._familiarity_penalty_stats["Artist B"]["percentile"], 0.25)
        self.assertAlmostEqual(daylist._familiarity_penalty_stats["Artist C"]["percentile"], 1.0)

    def test_playlist_state_round_trips_atomically(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            daylist = Daylist.__new__(Daylist)
            daylist.state_file = Path(temp_dir) / "nested" / "state.json"

            daylist._save_playlist_id("playlist-id")
            self.assertEqual(daylist._load_playlist_id(), "playlist-id")

    def test_invalid_playlist_state_returns_none(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_file = Path(temp_dir) / "state.json"
            state_file.write_text("[]", encoding="utf-8")
            daylist = Daylist.__new__(Daylist)
            daylist.state_file = state_file

            self.assertIsNone(daylist._load_playlist_id())


if __name__ == "__main__":
    unittest.main()