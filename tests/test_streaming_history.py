import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from StreamingHistory import StreamingHistoryParser


class StreamingHistoryTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.export_dir = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def write_export(self, records):
        path = self.export_dir / "Streaming_History_Audio_2026_0.json"
        path.write_text(json.dumps(records), encoding="utf-8")
        return path

    def test_parse_preserves_display_names_and_aggregates_plays(self):
        self.write_export([
            {
                "ts": "2026-01-01T12:00:00Z",
                "master_metadata_album_artist_name": "Beyoncé",
                "master_metadata_track_name": "Example Song",
                "ms_played": 120000,
                "skipped": False,
            },
            {
                "ts": "2026-01-02T12:00:00Z",
                "master_metadata_album_artist_name": "Beyoncé",
                "master_metadata_track_name": "Example Song",
                "ms_played": 90000,
                "skipped": True,
            },
        ])
        parser = StreamingHistoryParser(self.export_dir, now=datetime(2026, 2, 1, tzinfo=timezone.utc))
        stats = parser.parse()

        artist_key = "beyonce"
        track_key = ("beyonce", "example song")
        self.assertEqual(stats.artists[artist_key].play_count, 2)
        self.assertEqual(stats.artists[artist_key].skip_count, 1)
        self.assertEqual(stats.artists[artist_key].total_ms_played, 210000)
        self.assertEqual(stats.artist_display_names[artist_key], "Beyoncé")
        self.assertEqual(stats.track_display_names[track_key], "Example Song")
        self.assertEqual(stats.tracks[track_key].play_count, 2)

    def test_dst_transition_uses_named_timezone(self):
        parser = StreamingHistoryParser(self.export_dir, timezone_name="Europe/Berlin")
        self.assertEqual(parser._slot_and_weekday("2026-03-29T00:30:00Z"), ("night", "sunday"))
        self.assertEqual(parser._slot_and_weekday("2026-03-29T04:30:00Z"), ("morning", "sunday"))

    def test_future_timestamp_has_zero_weight(self):
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        parser = StreamingHistoryParser(self.export_dir, now=now)
        self.assertEqual(parser._decay_weight("2026-01-02T00:00:00Z"), 0.0)

    def test_invalid_ms_played_is_safe(self):
        self.write_export([{
            "ts": "2026-01-01T12:00:00Z",
            "master_metadata_album_artist_name": "Artist",
            "master_metadata_track_name": "Track",
            "ms_played": "invalid",
        }])
        stats = StreamingHistoryParser(
            self.export_dir,
            now=datetime(2026, 2, 1, tzinfo=timezone.utc),
        ).parse()
        self.assertEqual(stats.artists["artist"].total_ms_played, 0)

    def test_missing_export_files_raise(self):
        with self.assertRaises(FileNotFoundError):
            StreamingHistoryParser(self.export_dir).parse()

    def test_naive_reference_datetime_is_rejected(self):
        with self.assertRaises(ValueError):
            StreamingHistoryParser(self.export_dir, now=datetime(2026, 1, 1))

    def test_nonpositive_half_life_is_rejected(self):
        with self.assertRaises(ValueError):
            StreamingHistoryParser(self.export_dir, half_life_days=0)


if __name__ == "__main__":
    unittest.main()