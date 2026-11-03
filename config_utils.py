"""Shared JSON configuration loading and validation helpers."""

import inspect
import json
import logging
from pathlib import Path

logger = logging.getLogger("song_radio.config")


def keyword_parameters(callable_obj):
    """Return the keyword parameter names accepted by a callable."""
    signature = inspect.signature(callable_obj)
    return {
        name
        for name, parameter in signature.parameters.items()
        if name != "self"
        and parameter.kind in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        )
    }


def load_config(name="config", allowed_keys=None, required_keys=()):
    """Load <name>.json, falling back to <name>_template.json.

    Args:
        name: Config filename stem, without the .json suffix.
        allowed_keys: Optional iterable of recognized top-level keys.
        required_keys: Iterable of keys that must be present.

    Returns:
        The parsed configuration dictionary.

    Raises:
        RuntimeError: If no config file exists, JSON is invalid, the root
            value is not an object, or required keys are missing.
    """
    base = Path(name)
    candidates = [base.with_suffix(".json"), base.with_name(f"{base.name}_template.json")]

    config_path = next((path for path in candidates if path.is_file()), None)
    if config_path is None:
        names = " or ".join(str(path) for path in candidates)
        raise RuntimeError(f"Could not find {names}.")

    try:
        with config_path.open("r", encoding="utf-8") as file:
            config = json.load(file)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{config_path} contains invalid JSON: {exc}") from exc
    except OSError as exc:
        raise RuntimeError(f"Could not read {config_path}: {exc}") from exc

    if not isinstance(config, dict):
        raise RuntimeError(f"{config_path} must contain a JSON object.")

    missing = set(required_keys) - config.keys()
    if missing:
        raise RuntimeError(
            f"{config_path} is missing required key(s): {', '.join(sorted(missing))}"
        )

    if allowed_keys is not None:
        unknown = config.keys() - set(allowed_keys)
        for key in sorted(unknown):
            logger.warning("Ignoring unknown configuration key %r in %s.", key, config_path)

    return config