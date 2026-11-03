import unittest

from normalization import normalize_artist_name, normalize_name, normalize_track_title


class NormalizationTests(unittest.TestCase):
    def test_artist_name_ignores_case_punctuation_and_accents(self):
        self.assertEqual(normalize_artist_name("Beyoncé"), "beyonce")
        self.assertEqual(normalize_artist_name("AC/DC"), "ac dc")
        self.assertEqual(normalize_artist_name("AC DC"), "ac dc")

    def test_track_title_removes_version_suffixes(self):
        self.assertEqual(normalize_track_title("Example Song - 2011 Remastered"), "example song")
        self.assertEqual(normalize_track_title("Example Song (Live at Berlin)"), "example song")

    def test_track_title_preserves_named_remix(self):
        self.assertEqual(normalize_track_title("Example Song - PNAU Remix"), "example song pnau remix")

    def test_normalize_name_uses_unicode_casefolding(self):
        self.assertEqual(normalize_name("  Straße  "), "strasse")

    def test_non_string_values_return_empty_key(self):
        self.assertEqual(normalize_artist_name(None), "")
        self.assertEqual(normalize_track_title(None), "")
        self.assertEqual(normalize_name(None), "")


if __name__ == "__main__":
    unittest.main()