import json
import tempfile
import unittest
from pathlib import Path

from config_utils import keyword_parameters, load_config


class ConfigTests(unittest.TestCase):
    def test_load_config_reads_json_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            stem = Path(temp_dir) / "config"
            stem.with_suffix(".json").write_text(
                json.dumps({"client_id": "test"}),
                encoding="utf-8",
            )

            config = load_config(str(stem), required_keys={"client_id"})
            self.assertEqual(config, {"client_id": "test"})

    def test_load_config_uses_template_fallback(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            stem = Path(temp_dir) / "config"
            template = stem.with_name("config_template.json")
            template.write_text(json.dumps({"client_id": "template"}), encoding="utf-8")

            config = load_config(str(stem))
            self.assertEqual(config["client_id"], "template")

    def test_missing_required_key_raises(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            stem = Path(temp_dir) / "config"
            stem.with_suffix(".json").write_text("{}", encoding="utf-8")

            with self.assertRaises(RuntimeError):
                load_config(str(stem), required_keys={"client_id"})

    def test_unknown_key_emits_warning(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            stem = Path(temp_dir) / "config"
            stem.with_suffix(".json").write_text(
                json.dumps({"known": 1, "typo": 2}),
                encoding="utf-8",
            )

            with self.assertLogs("song_radio.config", level="WARNING"):
                load_config(str(stem), allowed_keys={"known"})

    def test_keyword_parameters_excludes_self(self):
        def example(self, first, second=1):
            return first + second

        self.assertEqual(keyword_parameters(example), {"first", "second"})


if __name__ == "__main__":
    unittest.main()