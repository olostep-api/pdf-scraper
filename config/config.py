import os
from dataclasses import dataclass

API_BASE = "https://api.olostep.com"
OUTPUT_DIR = "output"
DEFAULT_FORMATS = "markdown,text"
DEFAULT_OUT_FILE = "output.json"
DEFAULT_POLL_SECONDS = 5
DEFAULT_ITEMS_LIMIT = 50
LOG_LEVEL = "INFO"


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
        with open(path, "r", encoding="utf-8") as handle:
            for raw_line in handle:
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

    api_key = os.getenv("OLOSTEP_API_KEY", "")
    if not api_key:
        raise SystemExit("Missing OLOSTEP_API_KEY env var.")

    return Config(
        api_base=API_BASE,
        api_key=api_key,
        output_dir=OUTPUT_DIR,
        default_formats=DEFAULT_FORMATS,
        default_out_file=DEFAULT_OUT_FILE,
        default_poll_seconds=DEFAULT_POLL_SECONDS,
        default_items_limit=DEFAULT_ITEMS_LIMIT,
        log_level=LOG_LEVEL,
    )


def ensure_output_path(output_dir: str, filename: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    return os.path.join(output_dir, filename)
