import json
from os import makedirs
from os.path import join
from typing import Any, Dict, List, Optional


def load_urls(args) -> List[str]:
    urls: List[str] = []
    if args.url:
        urls.extend(args.url)

    if args.urls_file:
        with open(args.urls_file, "r", encoding="utf-8") as file:
            for line in file:
                url = line.strip()
                if url and not url.startswith("#"):
                    urls.append(url)

    seen = set()
    deduped = []
    for url in urls:
        if url not in seen:
            seen.add(url)
            deduped.append(url)
    return deduped


def write_outputs(
    out_json: str, payload: Dict[str, Any], save_dir: Optional[str], formats: List[str]
) -> None:
    with open(out_json, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)

    if not save_dir:
        return

    makedirs(save_dir, exist_ok=True)

    for idx, result in enumerate(payload.get("results", []), start=1):
        custom_id = result.get("custom_id") or f"pdf-{idx}"
        retrieved = result.get("retrieved") or {}
        for fmt in formats:
            key = f"{fmt}_content"
            content = retrieved.get(key)
            if isinstance(content, str) and content.strip():
                ext = "md" if fmt == "markdown" else "txt" if fmt == "text" else fmt
                path = join(save_dir, f"{custom_id}.{ext}")
                with open(path, "w", encoding="utf-8") as file:
                    file.write(content)


def normalize_retrieve_formats(formats: List[str]) -> Optional[List[str]]:
    allowed_formats = {"html", "markdown", "json"}
    filtered = [fmt for fmt in formats if fmt in allowed_formats]
    return filtered or None
