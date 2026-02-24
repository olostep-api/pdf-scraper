import argparse
import asyncio
import os
from datetime import datetime
from typing import List

import httpx
from loguru import logger

from config.config import load_config, ensure_output_path
from src.batch_scraper import BatchScraper
from src.single_pdf_scraper import scrape_single_pdf
from utils import load_urls, normalize_retrieve_formats, write_outputs


def setup_logger() -> None:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.remove()
    logger.add(
        sink=lambda msg: print(msg, end=""),
        level=log_level,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    )


async def main_async():
    setup_logger()
    cfg = load_config()
    logger.info("Configuration loaded. API base: {}", cfg.api_base)

    p = argparse.ArgumentParser(
        description="Minimal PDF scraper using Olostep (single -> scrapes, multi -> batches)."
    )
    p.add_argument("--url", action="append", help="PDF URL (repeatable).")
    p.add_argument("--urls-file", help="Text file with 1 URL per line.")
    p.add_argument(
        "--out",
        help="Output JSON filename (saved inside output folder). If omitted, auto-generated.",
    )
    p.add_argument(
        "--formats",
        default=cfg.default_formats,
        help="Comma-separated formats, e.g. markdown,text,html",
    )
    p.add_argument(
        "--poll-seconds",
        type=int,
        default=cfg.default_poll_seconds,
        help="Batch polling interval in seconds.",
    )
    p.add_argument(
        "--items-limit",
        type=int,
        default=cfg.default_items_limit,
        help="Batch items page size.",
    )
    args = p.parse_args()

    urls = load_urls(args)
    if not urls:
        raise SystemExit("Provide --url and/or --urls-file.")
    logger.info("Loaded {} URL(s)", len(urls))

    formats = [x.strip() for x in args.formats.split(",") if x.strip()]
    logger.info("Requested formats: {}", ",".join(formats))

    timestamp = datetime.now().strftime("%H-%M_%Y-%m-%d")
    if args.out:
        out_filename = args.out
    elif len(urls) == 1:
        out_filename = f"single_{timestamp}.json"
    else:
        out_filename = f"batch{len(urls)}_{timestamp}.json"

    out_json = ensure_output_path(cfg.output_dir, out_filename)
    save_dir = cfg.output_dir
    logger.info("Output JSON: {}", out_json)
    logger.info("Output files directory: {}", save_dir)

    async with httpx.AsyncClient() as client:
        if len(urls) == 1:
            logger.info("Running in single mode")
            scrape = await scrape_single_pdf(client, cfg, urls[0], formats=formats)
            scrape_result = scrape.get("result") or {}

            payload = {
                "mode": "single",
                "url": urls[0],
                "scrape": scrape,
                "results": [
                    {
                        "custom_id": "pdf-1",
                        "url": urls[0],
                        "retrieved": scrape_result,
                    }
                ],
            }
        else:
            logger.info("Running in batch mode")
            async with BatchScraper(
                api_token=cfg.api_key,
                base_url=cfg.api_base,
                timeout=60.0,
            ) as batch_client:
                batch = await batch_client.create_batch(urls)
                batch_id = batch.get("id")
                if not batch_id:
                    raise SystemExit(f"Batch create response missing id: {batch}")
                logger.info("Batch created: {}", batch_id)

                while True:
                    progress = await batch_client.get_batch_progress(batch_id)
                    if progress.status in ("completed", "failed", "cancelled"):
                        break
                    await asyncio.sleep(args.poll_seconds)

                results = []
                failed = []

                async for item in batch_client.iter_batch_items(
                    batch_id, status="completed", limit=args.items_limit
                ):
                    retrieve_id = item.get("retrieve_id") or item.get("retrieveId")
                    url = item.get("url")
                    custom_id = item.get("custom_id") or item.get("customId")
                    if retrieve_id:
                        retrieved = await batch_client.retrieve(
                            retrieve_id,
                            formats=normalize_retrieve_formats(formats),
                        )
                    else:
                        retrieved = None
                    results.append(
                        {
                            "custom_id": custom_id,
                            "url": url,
                            "retrieve_id": retrieve_id,
                            "retrieved": retrieved,
                            "raw_item": item,
                        }
                    )

                async for item in batch_client.iter_batch_items(
                    batch_id, status="failed", limit=args.items_limit
                ):
                    failed.append(item)

                payload = {
                    "mode": "batch",
                    "requested_count": len(urls),
                    "batch_id": batch_id,
                    "results": results,
                    "failed_items": failed,
                }
                logger.info(
                    "Batch finished: {} completed item(s), {} failed item(s)",
                    len(results),
                    len(failed),
                )

    write_outputs(out_json, payload, save_dir, formats=formats)
    logger.info("Wrote output JSON to {}", out_json)


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
