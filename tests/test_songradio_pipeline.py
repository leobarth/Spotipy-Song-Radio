import unittest
from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import patch

from SongRadio import SongRadio


class SongRadioPipelineTests(unittest.TestCase):
    def test_empty_spotify_search_does_not_crash(self):
        radio = SongRadio.__new__(SongRadio)
        radio.seed_queries = {"Example Artist": "Example Song"}
        radio.sp = SimpleNamespace(search=lambda **kwargs: {"tracks": {"items": []}})
        radio.request_sleep = 0

        self.assertEqual(radio.resolve_seed_track_ids(), [])

    def test_seed_search_selects_matching_artist_and_title(self):
        items = [
            {"id": "wrong", "name": "Different Song", "artists": [{"name": "Other Artist"}]},
            {"id": "right", "name": "Example Song", "artists": [{"name": "Example Artist"}]},
        ]
        radio = SongRadio.__new__(SongRadio)
        radio.seed_queries = {"Example Artist": "Example Song"}
        radio.sp = SimpleNamespace(search=lambda **kwargs: {"tracks": {"items": items}})
        radio.request_sleep = 0

        self.assertEqual(radio.resolve_seed_track_ids(), ["right"])

    def test_artist_cap_allows_next_artist_to_fill_result_limit(self):
        radio = SongRadio.__new__(SongRadio)
        radio.picks = []
        radio.seed_genre_set = {"rock"}
        radio.seed_similar_match = {}
        radio.similarity_blend_weight_range = (0.0, 0.0)
        radio.result_limit = 2
        radio.similarity_oversample_factor = 3
        radio.lastfm_batch_size = 10
        radio.lastfm_max_workers = 2
        radio.min_artist_listeners = 0
        radio.excluded_genres = set()
        radio.lastfm_listener_ceiling = 10000
        radio.min_track_listeners = 0
        radio.max_tracks_per_artist = 1
        radio.popularity_bias = 0
        radio._ensure_seed_similar_artists = lambda: None

        tracks = [
            {"id": "a1", "name": "Track A1", "duration_ms": 100000, "artists": [{"name": "Artist A"}]},
            {"id": "a2", "name": "Track A2", "duration_ms": 100000, "artists": [{"name": "Artist A"}]},
            {"id": "b1", "name": "Track B1", "duration_ms": 100000, "artists": [{"name": "Artist B"}]},
        ]
        radio._fetch_artist_infos_batch = lambda names, workers: {
            name: {"tags": {"rock"}, "listeners": 100, "top_tracks": set()}
            for name in names
        }

        def completed_futures(callables, workers):
            futures = []
            for _callable in callables:
                future = Future()
                future.set_result(100)
                futures.append(future)
            return futures

        radio._run_concurrent = completed_futures
        radio._lastfm_monitor = SimpleNamespace(
            recent_throttle_rate=0.0,
            throttle_rate_alert=0.15,
        )

        with patch("SongRadio.random.shuffle", side_effect=lambda values: None):
            picks = radio.filter_and_rank(tracks)

        artists = [pick["track"]["artists"][0]["name"] for pick in picks]
        self.assertEqual(len(picks), 2)
        self.assertEqual(len(set(artists)), 2)

    def test_lastfm_api_error_is_not_returned_as_successful_empty_data(self):
        response = SimpleNamespace(
            status_code=400,
            headers={},
            reason="Bad Request",
            json=lambda: {"error": 6, "message": "Invalid parameters"},
        )
        radio = SongRadio.__new__(SongRadio)
        radio.lastfm_max_retries = 1
        radio.lastfm_api_key = "test-api-key"
        radio._get_lastfm_session = lambda: SimpleNamespace(get=lambda *args, **kwargs: response)
        radio._lastfm_monitor = SimpleNamespace(record=lambda *args, **kwargs: None)

        self.assertIs(radio._lastfm_get("artist.getInfo"), radio._LASTFM_FAILURE)

    def test_retry_after_is_capped(self):
        responses = [
            SimpleNamespace(status_code=429, headers={"Retry-After": "999999"}, reason="Too Many Requests"),
            SimpleNamespace(status_code=200, headers={}, json=lambda: {"artist": {"name": "Example"}}),
        ]

        def get_response(*args, **kwargs):
            return responses.pop(0)

        outcomes = []
        radio = SongRadio.__new__(SongRadio)
        radio.lastfm_max_retries = 2
        radio.lastfm_api_key = "test-api-key"
        radio._get_lastfm_session = lambda: SimpleNamespace(get=get_response)
        radio._lastfm_monitor = SimpleNamespace(record=lambda *args, **kwargs: outcomes.append(args[0]))

        with patch("SongRadio.time.sleep") as sleep:
            result = radio._lastfm_get("artist.getInfo")

        self.assertEqual(result, {"artist": {"name": "Example"}})
        sleep.assert_called_once_with(radio.MAX_RETRY_AFTER_SECONDS)
        self.assertEqual(outcomes, ["throttled", "ok"])


if __name__ == "__main__":
    unittest.main()