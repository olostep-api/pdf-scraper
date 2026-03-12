import argparse
import asyncio

from loguru import logger

from config.config import DEFAULT_FORMATS, DEFAULT_ITEMS_LIMIT, DEFAULT_POLL_SECONDS, load_config
from src.workflow import RunRequest, run_scrape_workflow
from utils.pipeline_io import load_urls


def setup_logger(log_level: str) -> None:
    logger.remove()
    logger.add(
        sink=lambda msg: print(msg, end=""),
        level=log_level.upper(),
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Minimal PDF scraper using Olostep (single -> scrapes, multi -> batches)."
    )
    parser.add_argument("--url", action="append", help="PDF URL (repeatable).")
    parser.add_argument("--urls-file", help="Text file with 1 URL per line.")
    parser.add_argument(
        "--out",
        help="Output JSON filename (saved inside output folder). If omitted, auto-generated.",
    )
    parser.add_argument(
        "--formats",
        default=DEFAULT_FORMATS,
        help="Comma-separated formats, e.g. markdown,text,html",
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=DEFAULT_POLL_SECONDS,
        help="Batch polling interval in seconds.",
    )
    parser.add_argument(
        "--items-limit",
        type=int,
        default=DEFAULT_ITEMS_LIMIT,
        help="Batch items page size.",
    )
    return parser


async def main_async() -> None:
    args = build_parser().parse_args()
    cfg = load_config()
    setup_logger(cfg.log_level)
    logger.info("Configuration loaded. API base: {}", cfg.api_base)

    urls = load_urls(args)
    if not urls:
        raise SystemExit("Provide --url and/or --urls-file.")

    formats = [value.strip() for value in args.formats.split(",") if value.strip()]
    request = RunRequest(
        urls=urls,
        formats=formats,
        out_filename=args.out,
        poll_seconds=args.poll_seconds,
        items_limit=args.items_limit,
    )
    result = await run_scrape_workflow(request, cfg=cfg)
    logger.info("Wrote output JSON to {}", result.output_json)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
