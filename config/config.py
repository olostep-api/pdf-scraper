import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    api_base: str
    api_key: str
    output_dir: str
    default_formats: str
    default_out_file: str
    default_poll_seconds: int
    default_items_limit: int
    log_level: str


def load_dotenv(path: str = ".env") -> None:
    """Load only OLOSTEP_API_KEY from .env if it is not already set."""
    allowed_keys = {"OLOSTEP_API_KEY"}

    def strip_quotes(value: str) -> str:
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            return value[1:-1]
        return value

    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[len("export ") :].lstrip()
                if "=" not in line:
                    continue

                key, value = line.split("=", 1)
                key = key.strip()
                value = strip_quotes(value.strip())

                if key in allowed_keys and key not in os.environ:
                    os.environ[key] = value
    except FileNotFoundError:
        return


def load_config() -> Config:
    load_dotenv(".env")

    # Runtime defaults are controlled in code. Only API key is loaded from .env.
    api_base = "https://api.olostep.com"
    api_key = os.getenv("OLOSTEP_API_KEY", "")
    output_dir = "output"
    default_formats = "markdown,text"
    default_out_file = "output.json"
    default_poll_seconds = 5
    default_items_limit = 50
    log_level = "INFO"

    if not api_key:
        raise SystemExit("Missing OLOSTEP_API_KEY env var.")

    return Config(
        api_base=api_base,
        api_key=api_key,
        output_dir=output_dir,
        default_formats=default_formats,
        default_out_file=default_out_file,
        default_poll_seconds=default_poll_seconds,
        default_items_limit=default_items_limit,
        log_level=log_level,
    )


def ensure_output_path(output_dir: str, filename: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    return os.path.join(output_dir, filename)
