from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence

import httpx
from loguru import logger

from config.config import Config, ensure_output_path, load_config
from src.batch_scraper import BatchScraper
from src.single_pdf_scraper import scrape_single_pdf
from utils.pipeline_io import normalize_retrieve_formats, write_outputs

SUPPORTED_SINGLE_FORMATS = ("markdown", "text", "html", "json")
SUPPORTED_BATCH_FORMATS = ("markdown", "html", "json")
FORMAT_EXTENSIONS = {
    "markdown": "md",
    "text": "txt",
    "html": "html",
    "json": "json",
}
CONTENT_KEYS = {fmt: f"{fmt}_content" for fmt in FORMAT_EXTENSIONS}
HOSTED_KEYS = {fmt: f"{fmt}_hosted_url" for fmt in FORMAT_EXTENSIONS}
PLACEHOLDER_VALUES = {"not-implemented", "not implemented", "n/a", "none"}

ProgressCallback = Callable[["RunProgressEvent"], None]


@dataclass(frozen=True)
class RunRequest:
    urls: List[str]
    formats: List[str]
    out_filename: Optional[str] = None
    poll_seconds: int = 5
    items_limit: int = 50


@dataclass(frozen=True)
class RunProgressEvent:
    phase: str
    message: str
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))
    current: int = 0
    total: int = 0
    percent: float = 0.0
    status: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ResultRecord:
    custom_id: str
    url: str
    retrieve_id: Optional[str]
    status: str
    available_formats: List[str]
    content_source: Dict[str, str]
    preview_ready: bool
    hosted_links: Dict[str, str]
    local_files: Dict[str, str]
    inline_content: Dict[str, str]
    metadata: Dict[str, Any] = field(default_factory=dict)
    raw_item: Dict[str, Any] = field(default_factory=dict)
    retrieved: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RunResult:
    request: RunRequest
    mode: str
    payload: Dict[str, Any]
    output_json: str
    output_dir: str
    results: List[ResultRecord]
    events: List[RunProgressEvent]
    summary: Dict[str, Any]
    artifact_files: List[str]


class WorkflowError(RuntimeError):
    pass


def parse_urls_from_text(text: str) -> List[str]:
    return _dedupe_urls(text.splitlines())


def parse_urls_from_file_content(content: bytes | str) -> List[str]:
    if isinstance(content, bytes):
        decoded = content.decode("utf-8", errors="ignore")
    else:
        decoded = content
    return parse_urls_from_text(decoded)


def combine_urls(*collections: Iterable[str]) -> List[str]:
    merged: List[str] = []
    for collection in collections:
        merged.extend(collection)
    return _dedupe_urls(merged)


def infer_mode(urls: Sequence[str]) -> str:
    count = len(urls)
    if count == 0:
        return "empty"
    if count == 1:
        return "single"
    return "batch"


def allowed_formats_for_mode(mode: str) -> List[str]:
    if mode == "batch":
        return list(SUPPORTED_BATCH_FORMATS)
    return list(SUPPORTED_SINGLE_FORMATS)


def sanitize_formats_for_mode(formats: Sequence[str], mode: str) -> List[str]:
    allowed = set(allowed_formats_for_mode(mode))
    sanitized: List[str] = []
    for fmt in formats:
        value = fmt.strip().lower()
        if value and value in allowed and value not in sanitized:
            sanitized.append(value)
    return sanitized


def make_run_request(
    urls: Sequence[str],
    formats: Sequence[str],
    *,
    out_filename: Optional[str] = None,
    poll_seconds: int = 5,
    items_limit: int = 50,
) -> RunRequest:
    cleaned_urls = _dedupe_urls(urls)
    cleaned_formats: List[str] = []
    for fmt in formats:
        value = fmt.strip().lower()
        if value and value not in cleaned_formats:
            cleaned_formats.append(value)
    return RunRequest(
        urls=cleaned_urls,
        formats=cleaned_formats,
        out_filename=out_filename.strip() if out_filename else None,
        poll_seconds=int(poll_seconds),
        items_limit=int(items_limit),
    )


async def run_scrape_workflow(
    request: RunRequest,
    *,
    cfg: Optional[Config] = None,
    on_progress: Optional[ProgressCallback] = None,
) -> RunResult:
    runtime_cfg = cfg or load_config()
    events: List[RunProgressEvent] = []
    prepared = make_run_request(
        request.urls,
        request.formats,
        out_filename=request.out_filename,
        poll_seconds=request.poll_seconds,
        items_limit=request.items_limit,
    )

    if not prepared.urls:
        raise WorkflowError("Provide at least one PDF URL.")
    if not prepared.formats:
        raise WorkflowError("Choose at least one output format.")

    mode = infer_mode(prepared.urls)

    def emit(
        phase: str,
        message: str,
        *,
        current: int = 0,
        total: int = 0,
        status: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> None:
        percent = float(current / total) if total else 0.0
        event = RunProgressEvent(
            phase=phase,
            message=message,
            current=current,
            total=total,
            percent=percent,
            status=status,
            metadata=dict(metadata or {}),
        )
        events.append(event)
        if on_progress:
            on_progress(event)

    emit(
        "validating",
        f"Validated {len(prepared.urls)} unique URL(s) for {mode} mode.",
        current=len(prepared.urls),
        total=len(prepared.urls),
        status="ready",
    )

    timestamp = datetime.now().strftime("%H-%M_%Y-%m-%d")
    if prepared.out_filename:
        out_filename = prepared.out_filename
    elif mode == "single":
        out_filename = f"single_{timestamp}.json"
    else:
        out_filename = f"batch{len(prepared.urls)}_{timestamp}.json"

    out_json = ensure_output_path(runtime_cfg.output_dir, out_filename)
    save_dir = runtime_cfg.output_dir
    payload: Dict[str, Any]

    try:
        async with httpx.AsyncClient() as client:
            if mode == "single":
                emit("submitting", "Submitting PDF scrape.", current=0, total=1, status="running")
                scrape = await scrape_single_pdf(
                    client,
                    runtime_cfg,
                    prepared.urls[0],
                    formats=prepared.formats,
                )
                scrape_result = scrape.get("result") or {}
                payload = {
                    "mode": "single",
                    "url": prepared.urls[0],
                    "scrape": scrape,
                    "results": [
                        {
                            "custom_id": "pdf-1",
                            "url": prepared.urls[0],
                            "retrieved": scrape_result,
                        }
                    ],
                }
                emit("retrieving", "Retrieved single scrape result.", current=1, total=1, status="completed")
            else:
                emit(
                    "submitting",
                    f"Creating batch for {len(prepared.urls)} PDFs.",
                    current=0,
                    total=len(prepared.urls),
                    status="running",
                )
                async with BatchScraper(
                    api_token=runtime_cfg.api_key,
                    base_url=runtime_cfg.api_base,
                    timeout=60.0,
                ) as batch_client:
                    batch = await batch_client.create_batch(prepared.urls)
                    batch_id = batch.get("id")
                    if not batch_id:
                        raise WorkflowError(f"Batch create response missing id: {batch}")

                    emit(
                        "polling",
                        f"Batch {batch_id} submitted. Waiting for completion.",
                        current=0,
                        total=len(prepared.urls),
                        status="queued",
                        metadata={"batch_id": batch_id},
                    )

                    while True:
                        progress = await batch_client.get_batch_progress(batch_id)
                        emit(
                            "polling",
                            f"Batch status: {progress.status} ({progress.completed_urls}/{progress.total_urls}).",
                            current=progress.completed_urls,
                            total=progress.total_urls,
                            status=progress.status,
                            metadata={"batch_id": batch_id},
                        )
                        if progress.status in ("completed", "failed", "cancelled"):
                            break
                        await asyncio.sleep(prepared.poll_seconds)

                    results: List[Dict[str, Any]] = []
                    failed: List[Dict[str, Any]] = []
                    completed_count = 0
                    batch_total = max(len(prepared.urls), 1)

                    async for item in batch_client.iter_batch_items(
                        batch_id,
                        status="completed",
                        limit=prepared.items_limit,
                    ):
                        retrieve_id = item.get("retrieve_id") or item.get("retrieveId")
                        url = item.get("url")
                        custom_id = item.get("custom_id") or item.get("customId")
                        if retrieve_id:
                            retrieved = await batch_client.retrieve(
                                retrieve_id,
                                formats=normalize_retrieve_formats(prepared.formats),
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
                        completed_count += 1
                        emit(
                            "retrieving",
                            f"Retrieved item {completed_count} of {batch_total}.",
                            current=completed_count,
                            total=batch_total,
                            status="running",
                            metadata={"batch_id": batch_id, "custom_id": custom_id},
                        )

                    async for item in batch_client.iter_batch_items(
                        batch_id,
                        status="failed",
                        limit=prepared.items_limit,
                    ):
                        failed.append(item)

                    payload = {
                        "mode": "batch",
                        "requested_count": len(prepared.urls),
                        "batch_id": batch_id,
                        "results": results,
                        "failed_items": failed,
                    }
                    emit(
                        "retrieving",
                        f"Batch retrieval finished with {len(results)} completed and {len(failed)} failed item(s).",
                        current=len(results),
                        total=len(prepared.urls),
                        status="completed" if not failed else "partial",
                        metadata={"batch_id": batch_id},
                    )

        emit("persisting", f"Writing aggregate JSON to {out_json}.", current=1, total=1, status="running")
        write_outputs(out_json, payload, save_dir, formats=prepared.formats)
        result = build_run_result(
            request=prepared,
            payload=payload,
            output_json=out_json,
            output_dir=save_dir,
            events=events,
            requested_formats=prepared.formats,
        )
        emit(
            "complete",
            f"Run complete. Saved output to {out_json}.",
            current=result.summary.get("completed_count", 0),
            total=max(result.summary.get("requested_count", 0), 1),
            status=result.summary.get("status"),
        )
        return build_run_result(
            request=prepared,
            payload=payload,
            output_json=out_json,
            output_dir=save_dir,
            events=events,
            requested_formats=prepared.formats,
        )
    except Exception as exc:
        emit("error", str(exc), status="error")
        raise


def run_scrape_workflow_sync(
    request: RunRequest,
    *,
    cfg: Optional[Config] = None,
    on_progress: Optional[ProgressCallback] = None,
) -> RunResult:
    return asyncio.run(run_scrape_workflow(request, cfg=cfg, on_progress=on_progress))


def build_run_result(
    *,
    request: RunRequest,
    payload: Dict[str, Any],
    output_json: str,
    output_dir: str,
    events: Sequence[RunProgressEvent],
    requested_formats: Optional[Sequence[str]] = None,
) -> RunResult:
    formats = list(requested_formats or request.formats or infer_formats_from_payload(payload, output_dir))
    records = normalize_payload(payload, output_dir, formats)
    artifact_files = collect_artifact_files(output_json, records)
    summary = summarize_records(payload, records, artifact_files)
    return RunResult(
        request=request,
        mode=str(payload.get("mode") or infer_mode(request.urls)),
        payload=payload,
        output_json=output_json,
        output_dir=output_dir,
        results=records,
        events=list(events),
        summary=summary,
        artifact_files=artifact_files,
    )


def load_run_result_from_file(path: str | Path) -> RunResult:
    run_path = Path(path)
    with run_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    output_dir = str(run_path.parent)
    formats = infer_formats_from_payload(payload, output_dir)
    urls = _extract_urls_from_payload(payload)
    request = make_run_request(urls, formats, out_filename=run_path.name)
    return build_run_result(
        request=request,
        payload=payload,
        output_json=str(run_path),
        output_dir=output_dir,
        events=[],
        requested_formats=formats,
    )


def list_saved_runs(output_dir: str | Path) -> List[Dict[str, Any]]:
    root = Path(output_dir)
    if not root.exists():
        return []

    runs: List[Dict[str, Any]] = []
    for path in sorted(root.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
        try:
            result = load_run_result_from_file(path)
        except Exception as exc:
            runs.append(
                {
                    "name": path.name,
                    "path": str(path),
                    "modified_at": datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds"),
                    "mode": "unknown",
                    "status": "error",
                    "error": str(exc),
                }
            )
            continue

        runs.append(
            {
                "name": path.name,
                "path": str(path),
                "modified_at": datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds"),
                "mode": result.mode,
                "status": result.summary.get("status", "unknown"),
                "requested_count": result.summary.get("requested_count", 0),
                "completed_count": result.summary.get("completed_count", 0),
                "failed_count": result.summary.get("failed_count", 0),
                "preview_ready_count": result.summary.get("preview_ready_count", 0),
                "hosted_only_count": result.summary.get("hosted_only_count", 0),
                "artifact_count": result.summary.get("artifact_count", 0),
            }
        )
    return runs


def infer_formats_from_payload(payload: Mapping[str, Any], output_dir: str | Path) -> List[str]:
    detected: List[str] = []
    output_root = Path(output_dir)
    for result in payload.get("results", []) or []:
        custom_id = str(result.get("custom_id") or "")
        retrieved = result.get("retrieved") or {}
        if isinstance(retrieved, Mapping):
            for fmt in FORMAT_EXTENSIONS:
                content = _normalize_content_value(retrieved.get(CONTENT_KEYS[fmt]))
                hosted = _normalize_url_value(retrieved.get(HOSTED_KEYS[fmt]))
                if content or hosted:
                    _append_unique(detected, fmt)
        if custom_id:
            for fmt, ext in FORMAT_EXTENSIONS.items():
                for candidate in _local_file_candidates(output_root, custom_id, fmt):
                    if candidate.exists():
                        _append_unique(detected, fmt)
                        break

    return detected or ["markdown"]


def normalize_payload(
    payload: Mapping[str, Any],
    output_dir: str | Path,
    requested_formats: Optional[Sequence[str]] = None,
) -> List[ResultRecord]:
    output_root = Path(output_dir)
    normalized: List[ResultRecord] = []
    formats = list(requested_formats or infer_formats_from_payload(payload, output_root))

    for item in payload.get("results", []) or []:
        normalized.append(normalize_result_record(item, output_root, formats))

    for item in payload.get("failed_items", []) or []:
        normalized.append(
            ResultRecord(
                custom_id=str(item.get("custom_id") or item.get("customId") or item.get("url") or "failed-item"),
                url=str(item.get("url") or ""),
                retrieve_id=item.get("retrieve_id") or item.get("retrieveId"),
                status="failed",
                available_formats=[],
                content_source={},
                preview_ready=False,
                hosted_links={},
                local_files={},
                inline_content={},
                metadata={"bucket": "failed_items", "raw": dict(item)},
                raw_item=dict(item),
                retrieved={},
            )
        )

    return normalized


def normalize_result_record(
    item: Mapping[str, Any],
    output_dir: str | Path,
    requested_formats: Optional[Sequence[str]] = None,
) -> ResultRecord:
    output_root = Path(output_dir)
    custom_id = str(item.get("custom_id") or item.get("customId") or item.get("url") or "item")
    retrieved_obj = item.get("retrieved") or {}
    retrieved: Dict[str, Any]
    if isinstance(retrieved_obj, Mapping):
        retrieved = dict(retrieved_obj)
    else:
        retrieved = {}

    formats = list(requested_formats or infer_formats_from_payload({"results": [item]}, output_root))
    available_formats: List[str] = []
    content_source: Dict[str, str] = {}
    hosted_links: Dict[str, str] = {}
    local_files: Dict[str, str] = {}
    inline_content: Dict[str, str] = {}

    for fmt in formats:
        inline_value = _normalize_content_value(retrieved.get(CONTENT_KEYS[fmt]))
        hosted_value = _normalize_url_value(retrieved.get(HOSTED_KEYS[fmt]))
        local_path = find_local_file(output_root, custom_id, fmt)
        local_preview = read_local_preview(local_path) if local_path else None

        if inline_value is not None:
            inline_content[fmt] = inline_value
            content_source[fmt] = "inline"
            _append_unique(available_formats, fmt)
        elif local_preview is not None:
            local_files[fmt] = str(local_path)
            content_source[fmt] = "local"
            _append_unique(available_formats, fmt)
        elif hosted_value is not None:
            hosted_links[fmt] = hosted_value
            if local_path:
                local_files[fmt] = str(local_path)
            content_source[fmt] = "hosted"
            _append_unique(available_formats, fmt)
        else:
            if local_path:
                local_files[fmt] = str(local_path)
            content_source[fmt] = "unavailable"

        if local_path and fmt not in local_files:
            local_files[fmt] = str(local_path)
        if hosted_value and fmt not in hosted_links:
            hosted_links[fmt] = hosted_value

    preview_ready = any(source in {"inline", "local"} for source in content_source.values())
    has_hosted_only = any(source == "hosted" for source in content_source.values())
    if preview_ready:
        status = "ready"
    elif has_hosted_only:
        status = "hosted-only"
    else:
        status = "unavailable"

    metadata = {
        "bucket": "results",
        "success": bool(retrieved.get("success", False)) if retrieved else False,
        "size_exceeded": bool(retrieved.get("size_exceeded", False)) if retrieved else False,
        "page_metadata": retrieved.get("page_metadata") or {},
        "raw_item": dict(item.get("raw_item") or {}),
    }

    return ResultRecord(
        custom_id=custom_id,
        url=str(item.get("url") or ""),
        retrieve_id=item.get("retrieve_id") or item.get("retrieveId"),
        status=status,
        available_formats=available_formats,
        content_source=content_source,
        preview_ready=preview_ready,
        hosted_links=hosted_links,
        local_files=local_files,
        inline_content=inline_content,
        metadata=metadata,
        raw_item=dict(item.get("raw_item") or item),
        retrieved=retrieved,
    )


def find_local_file(output_dir: str | Path, custom_id: str, fmt: str) -> Optional[Path]:
    root = Path(output_dir)
    for candidate in _local_file_candidates(root, custom_id, fmt):
        if candidate.exists():
            return candidate
    return None


def read_local_preview(path: str | Path | None) -> Optional[str]:
    if not path:
        return None
    preview_path = Path(path)
    if not preview_path.exists() or not preview_path.is_file():
        return None
    try:
        content = preview_path.read_text(encoding="utf-8")
    except OSError:
        return None
    return _normalize_content_value(content)


def resolve_preview_content(
    record: ResultRecord,
    format_name: str,
    *,
    hosted_cache: Optional[MutableMapping[str, str]] = None,
    fetch_hosted: bool = False,
    timeout: float = 30.0,
) -> Dict[str, Any]:
    format_name = format_name.strip().lower()
    if format_name not in FORMAT_EXTENSIONS:
        return {"source": "missing", "content": None, "external_url": None, "error": "Unsupported format."}

    inline_value = record.inline_content.get(format_name)
    if inline_value is not None:
        return {"source": "inline", "content": inline_value, "external_url": None, "error": None}

    local_path = record.local_files.get(format_name)
    local_value = read_local_preview(local_path)
    if local_value is not None:
        return {"source": "local", "content": local_value, "external_url": None, "error": None}

    hosted_url = record.hosted_links.get(format_name)
    if hosted_url and hosted_cache is not None and hosted_url in hosted_cache:
        cached = _normalize_content_value(hosted_cache[hosted_url])
        if cached is not None:
            return {"source": "hosted", "content": cached, "external_url": hosted_url, "error": None}

    if hosted_url and fetch_hosted:
        try:
            with httpx.Client(timeout=timeout, follow_redirects=True) as client:
                response = client.get(hosted_url)
                response.raise_for_status()
            fetched = _normalize_content_value(response.text)
            if fetched is not None:
                if hosted_cache is not None:
                    hosted_cache[hosted_url] = fetched
                return {"source": "hosted", "content": fetched, "external_url": hosted_url, "error": None}
            return {
                "source": "external",
                "content": None,
                "external_url": hosted_url,
                "error": "Hosted content is empty or unavailable.",
            }
        except Exception as exc:
            return {
                "source": "external",
                "content": None,
                "external_url": hosted_url,
                "error": str(exc),
            }

    if hosted_url:
        return {"source": "external", "content": None, "external_url": hosted_url, "error": None}

    return {"source": "missing", "content": None, "external_url": None, "error": None}


def summarize_records(
    payload: Mapping[str, Any],
    records: Sequence[ResultRecord],
    artifact_files: Sequence[str],
) -> Dict[str, Any]:
    completed_count = sum(1 for record in records if record.metadata.get("bucket") == "results")
    failed_count = sum(1 for record in records if record.status == "failed")
    preview_ready_count = sum(1 for record in records if record.preview_ready)
    hosted_only_count = sum(1 for record in records if record.status == "hosted-only")
    unavailable_count = sum(1 for record in records if record.status == "unavailable")
    requested_count = int(payload.get("requested_count") or len(_extract_urls_from_payload(payload)) or completed_count + failed_count)

    if failed_count and completed_count:
        status = "partial"
    elif failed_count and not completed_count:
        status = "error"
    elif hosted_only_count and preview_ready_count < completed_count:
        status = "partial"
    elif unavailable_count and completed_count:
        status = "partial"
    elif completed_count:
        status = "success"
    else:
        status = "empty"

    return {
        "mode": str(payload.get("mode") or "unknown"),
        "status": status,
        "requested_count": requested_count,
        "completed_count": completed_count,
        "failed_count": failed_count,
        "preview_ready_count": preview_ready_count,
        "hosted_only_count": hosted_only_count,
        "unavailable_count": unavailable_count,
        "artifact_count": len(artifact_files),
        "batch_id": payload.get("batch_id"),
    }


def collect_artifact_files(output_json: str, records: Sequence[ResultRecord]) -> List[str]:
    artifacts = [str(Path(output_json))]
    for record in records:
        for path in record.local_files.values():
            if path:
                _append_unique(artifacts, str(Path(path)))
    return artifacts


def _extract_urls_from_payload(payload: Mapping[str, Any]) -> List[str]:
    if payload.get("mode") == "single":
        url = payload.get("url")
        return [str(url)] if url else []

    urls: List[str] = []
    for bucket in (payload.get("results", []) or [], payload.get("failed_items", []) or []):
        for item in bucket:
            url = item.get("url")
            if url:
                _append_unique(urls, str(url))
    return urls


def _local_file_candidates(output_dir: Path, custom_id: str, fmt: str) -> List[Path]:
    extension = FORMAT_EXTENSIONS[fmt]
    return [
        output_dir / f"{custom_id}.{extension}",
        output_dir / "files" / f"{custom_id}.{extension}",
    ]


def _normalize_content_value(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    if not stripped:
        return None
    if stripped.lower() in PLACEHOLDER_VALUES:
        return None
    return value


def _normalize_url_value(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _dedupe_urls(lines: Iterable[str]) -> List[str]:
    urls: List[str] = []
    seen = set()
    for raw in lines:
        value = str(raw).strip()
        if not value or value.startswith("#"):
            continue
        if value not in seen:
            seen.add(value)
            urls.append(value)
    return urls


def _append_unique(items: List[str], value: str) -> None:
    if value not in items:
        items.append(value)
