from typing import List, Dict, Any

import httpx
from loguru import logger

from config.config import Config


def auth_headers(cfg: Config) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {cfg.api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


async def scrape_single_pdf(
    client: httpx.AsyncClient, cfg: Config, url: str, formats: List[str]
) -> Dict[str, Any]:
    logger.info("Starting single scrape for URL: {}", url)
    payload = {
        "url_to_scrape": url,
        "formats": formats,
    }
    r = await client.post(
        f"{cfg.api_base}/v1/scrapes",
        headers=auth_headers(cfg),
        json=payload,
        timeout=(10.0, 60.0),  # connect timeout, read timeout
    )
    r.raise_for_status()
    logger.info("Single scrape completed for URL: {} (status={})", url, r.status_code)
    return r.json()
