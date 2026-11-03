import tempfile
import unittest
from pathlib import Path

from SongRadio import SongRadio
from StreamingHistory import EntityStats, StreamingHistoryStats, TimeBuckets


def sample_stats(artist_name="Beyoncé", track_name="Example Song"):
    artist_key = "beyonce"
    track_key = (artist_key, "example song")
    buckets = TimeBuckets()
    buckets.by_slot["evening"] = 2.0
    buckets.by_weekday["friday"] = 3.0
    buckets.by_slot_weekday[("evening", "friday")] = 1.5

    return StreamingHistoryStats(
        artists={artist_key: EntityStats(play_count=2, weighted_score=4.5)},
        tracks={track_key: EntityStats(play_count=2, weighted_score=3.0)},
        podcast_shows={},
        audiobooks={},
        artist_display_names={artist_key: artist_name},
        track_display_names={track_key: track_name},
        artist_time_buckets={artist_key: buckets},
    )


class SongRadioDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "cache.db"
        self.db = SongRadio._init_search_db(str(self.db_path))
        self.radio = SongRadio.__new__(SongRadio)
        self.radio._search_db = self.db

    def tearDown(self):
        self.db.close()
        self.temp_dir.cleanup()

    def test_history_import_persists_original_names_and_scores(self):
        self.radio.load_streaming_history(sample_stats())

        self.assertEqual(self.radio.get_familiarity_scores(), {"beyonce": 4.5})
        self.assertEqual(
            self.radio.get_top_tracks_by_artist(),
            {"beyonce": ("Beyoncé", "Example Song")},
        )
        self.assertEqual(
            self.radio.get_time_bucket_scores("evening", "friday")["beyonce"],
            {"slot_score": 2.0, "weekday_score": 3.0, "joint_score": 1.5},
        )

    def test_import_replaces_previous_history(self):
        self.radio.load_streaming_history(sample_stats())
        self.radio.load_streaming_history(sample_stats("New Artist", "New Track"))

        scores = self.radio.get_familiarity_scores()
        self.assertEqual(set(scores), {"beyonce"})
        self.assertEqual(
            self.radio.get_top_tracks_by_artist()["beyonce"],
            ("New Artist", "New Track"),
        )

    def test_failed_import_rolls_back_deletions(self):
        self.radio.load_streaming_history(sample_stats())
        self.db.execute(
            "CREATE TRIGGER reject_history_track BEFORE INSERT ON streaming_history_tracks "
            "BEGIN SELECT RAISE(ABORT, 'test failure'); END"
        )
        self.db.commit()

        with self.assertRaises(Exception):
            self.radio.load_streaming_history(sample_stats("Other Artist", "Other Track"))

        self.assertEqual(self.radio.get_familiarity_scores(), {"beyonce": 4.5})
        self.assertEqual(
            self.radio.get_top_tracks_by_artist()["beyonce"],
            ("Beyoncé", "Example Song"),
        )

    def test_memory_database_is_rejected(self):
        with self.assertRaises(ValueError):
            SongRadio._init_search_db(":memory:")


if __name__ == "__main__":
    unittest.main()