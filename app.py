from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from config.config import (
    DEFAULT_FORMATS,
    DEFAULT_ITEMS_LIMIT,
    DEFAULT_POLL_SECONDS,
    OUTPUT_DIR,
    load_config,
    load_dotenv,
)
from src.workflow import (
    ResultRecord,
    RunProgressEvent,
    RunRequest,
    RunResult,
    allowed_formats_for_mode,
    combine_urls,
    infer_mode,
    list_saved_runs,
    load_run_result_from_file,
    parse_urls_from_file_content,
    parse_urls_from_text,
    resolve_preview_content,
    run_scrape_workflow_sync,
)

FORMAT_ORDER = ["markdown", "text", "html", "json"]
STATUS_LABELS = {
    "success": "Success",
    "partial": "Partial",
    "error": "Error",
    "empty": "Idle",
    "ready": "Ready",
    "hosted-only": "Hosted only",
    "unavailable": "Unavailable",
    "failed": "Failed",
}
STATUS_CLASS = {
    "success": "success",
    "partial": "warning",
    "error": "danger",
    "empty": "neutral",
    "ready": "success",
    "hosted-only": "warning",
    "unavailable": "neutral",
    "failed": "danger",
}


def main() -> None:
    st.set_page_config(
        page_title="PDF Scraper",
        page_icon="PDF",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    load_dotenv(".env")
    _init_session_state()
    _apply_theme()

    api_ready = bool(os.getenv("OLOSTEP_API_KEY", "").strip())
    _render_header()

    tab_new, tab_saved = st.tabs(["Create Run", "History"])
    with tab_new:
        _render_new_run_tab(api_ready)
    with tab_saved:
        _render_saved_runs_tab()


def _init_session_state() -> None:
    st.session_state.setdefault("hosted_cache", {})
    st.session_state.setdefault("selected_formats", DEFAULT_FORMATS.split(","))
    st.session_state.setdefault("url_text", "")
    st.session_state.setdefault("output_filename", "")
    st.session_state.setdefault("poll_seconds", DEFAULT_POLL_SECONDS)
    st.session_state.setdefault("items_limit", DEFAULT_ITEMS_LIMIT)
    st.session_state.setdefault("active_result", None)
    st.session_state.setdefault("active_origin", None)
    st.session_state.setdefault("run_error", None)


def _apply_theme() -> None:
    st.markdown(
        """
        <style>
          @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;600;700&family=DM+Sans:wght@400;500;700&display=swap');

          :root {
            --primary: #635bff;
            --primary-hover: #534ae6;
            --primary-2: #7a74ff;
            --bg: #f8f9fc;
            --surface: #ffffff;
            --surface-soft: #f3f6ff;
            --text: #10142a;
            --muted: #465067;
            --border: #cdd7f0;
            --accent-1: #edf0ff;
            --accent-2: #e6eaff;
            --accent-3: #eff2ff;
            --accent-4: #eef1ff;
            --accent-5: #f5f7ff;
            --success: #0e7a53;
            --error: #c2362b;
            --warning: #7a5800;
            --radius: 14px;
          }

          html, body, [class*="css"] {
            font-family: "DM Sans", "Segoe UI", sans-serif;
            color: var(--text);
          }

          h1, h2, h3 {
            font-family: "Space Grotesk", "DM Sans", sans-serif;
            letter-spacing: -0.02em;
          }

          [data-testid="stAppViewContainer"] {
            background: linear-gradient(
              155deg,
              #ffffff 0%,
              #f4f6ff 35%,
              #ecefff 68%,
              #e4e9ff 100%
            );
          }

          [data-testid="stHeader"] {
            background: rgba(248, 249, 252, 0.9);
            border-bottom: 1px solid var(--border);
            backdrop-filter: blur(10px);
          }

          .block-container {
            max-width: 1180px;
            padding-top: 2.25rem;
            padding-bottom: 2rem;
          }

          div[data-testid="stVerticalBlockBorderWrapper"] {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: var(--radius);
            box-shadow: 0 1px 2px rgba(16, 20, 42, 0.04);
          }

          .hero-shell {
            border: 1px solid var(--border);
            border-radius: 18px;
            padding: 1.1rem 1.2rem;
            background: linear-gradient(
              138deg,
              #eceffd 0%,
              #e7ebff 34%,
              #dfe4ff 68%,
              #d6dcff 100%
            );
            animation: fadeIn 280ms ease-out;
          }

          .hero-kicker {
            color: var(--primary);
            font-size: 0.75rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            font-weight: 700;
            margin-bottom: 0.55rem;
          }

          .hero-title {
            margin: 0;
            font-size: clamp(2.15rem, 2.4vw, 2.85rem);
            line-height: 1.1;
            color: var(--text);
          }

          .hero-copy {
            margin-top: 0.52rem;
            color: var(--muted);
            font-size: 0.96rem;
            line-height: 1.55;
            max-width: 45rem;
          }

          .chip {
            display: inline-flex;
            align-items: center;
            gap: 0.35rem;
            border-radius: 999px;
            font-size: 0.79rem;
            font-weight: 700;
            border: 1px solid var(--border);
            padding: 0.3rem 0.62rem;
            color: var(--muted);
            background: var(--accent-5);
          }

          .chip.success { color: var(--success); background: #effaf5; border-color: #cfeadf; }
          .chip.warning { color: var(--warning); background: #fff7e5; border-color: #e6d19c; }
          .chip.danger { color: var(--error); background: #fff1f0; border-color: #f2cac6; }
          .chip.neutral { color: var(--muted); background: var(--accent-5); border-color: var(--border); }

          .label-title {
            color: var(--muted);
            text-transform: uppercase;
            font-size: 0.78rem;
            letter-spacing: 0.07em;
            font-weight: 700;
            margin-bottom: 0.5rem;
          }

          .stat-row {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 0.48rem;
          }

          .stat-tile {
            border: 1px solid var(--border);
            border-radius: 12px;
            background: var(--surface);
            padding: 0.6rem 0.72rem;
          }

          .stat-key {
            font-size: 0.7rem;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--muted);
            font-weight: 700;
            margin-bottom: 0.34rem;
          }

          .stat-value {
            font-family: "Space Grotesk", "DM Sans", sans-serif;
            color: var(--text);
            font-size: 1.12rem;
            line-height: 1;
            font-weight: 700;
          }

          .stat-note {
            margin-top: 0.24rem;
            color: var(--muted);
            font-size: 0.77rem;
          }

          .stButton > button,
          .stDownloadButton > button,
          [data-testid="stFormSubmitButton"] button {
            min-height: 2.45rem;
            border-radius: 11px;
            border: 1px solid var(--border) !important;
            box-shadow: none;
            font-weight: 600;
            background: var(--surface) !important;
            color: var(--text) !important;
          }

          .stButton > button[kind="primary"],
          [data-testid="stFormSubmitButton"] button[kind="primary"],
          button[data-testid="stBaseButton-primary"] {
            background: linear-gradient(135deg, var(--primary) 0%, var(--primary-2) 100%) !important;
            border-color: var(--primary) !important;
            color: #ffffff !important;
          }

          .stButton > button[kind="primary"]:hover,
          [data-testid="stFormSubmitButton"] button[kind="primary"]:hover,
          button[data-testid="stBaseButton-primary"]:hover {
            background: linear-gradient(135deg, var(--primary-hover) 0%, var(--primary) 100%) !important;
            border-color: var(--primary-hover) !important;
            color: #ffffff !important;
          }

          [data-testid="stFormSubmitButton"] {
            width: 100%;
          }

          [data-testid="stFormSubmitButton"] button {
            background: linear-gradient(135deg, var(--primary) 0%, var(--primary-2) 100%) !important;
            border-color: var(--primary) !important;
            color: #ffffff !important;
          }

          [data-testid="stFormSubmitButton"] button:hover {
            background: linear-gradient(135deg, var(--primary-hover) 0%, var(--primary) 100%) !important;
            border-color: var(--primary-hover) !important;
            color: #ffffff !important;
          }

          [data-testid="stFormSubmitButton"] button:disabled {
            background: #e7ebfa !important;
            border-color: #d2d9f3 !important;
            color: #5e6782 !important;
            opacity: 1;
          }

          [data-testid="stLinkButton"] a {
            min-height: 2.25rem;
            border-radius: 11px;
            border: 1px solid var(--border) !important;
            background: var(--surface) !important;
            color: var(--text) !important;
            font-weight: 600;
            text-decoration: none !important;
          }

          [data-testid="stLinkButton"] a:hover {
            border-color: var(--accent-2) !important;
            background: var(--surface-soft) !important;
            color: var(--text) !important;
          }

          .stTextInput input,
          .stNumberInput input,
          div[data-baseweb="select"] > div {
            border-radius: 11px;
            border-color: var(--border) !important;
            background: var(--surface);
            color: var(--text) !important;
            -webkit-text-fill-color: var(--text) !important;
          }

          .stTextArea textarea {
            border-radius: 11px;
            border-color: var(--border) !important;
            background: var(--surface) !important;
            color: var(--text) !important;
            -webkit-text-fill-color: var(--text) !important;
            caret-color: var(--primary) !important;
            font-weight: 500;
          }

          .stTextArea textarea::placeholder {
            color: #73819f !important;
            opacity: 1;
          }

          section[data-testid="stFileUploaderDropzone"] {
            border: 1px dashed var(--accent-2) !important;
            background: linear-gradient(180deg, var(--surface-soft), var(--accent-5)) !important;
            border-radius: 12px !important;
          }

          section[data-testid="stFileUploaderDropzone"] [data-testid="stMarkdownContainer"] p,
          section[data-testid="stFileUploaderDropzone"] small {
            color: var(--muted) !important;
          }

          section[data-testid="stFileUploaderDropzone"] button {
            background: var(--surface) !important;
            color: var(--text) !important;
            border: 1px solid var(--border) !important;
            border-radius: 10px !important;
          }

          [data-baseweb="tag"] {
            background: var(--accent-2) !important;
            border: 1px solid var(--accent-2) !important;
            border-radius: 10px !important;
          }

          [data-baseweb="tag"] * {
            color: var(--primary) !important;
          }

          code {
            background: var(--accent-3) !important;
            color: var(--text) !important;
            border: 1px solid var(--border);
            border-radius: 6px;
          }

          [data-testid="stCodeBlock"] {
            border: 1px solid var(--border);
            border-radius: 12px;
            background: var(--surface-soft) !important;
          }

          [data-testid="stCodeBlock"] pre,
          [data-testid="stCodeBlock"] code {
            color: var(--text) !important;
            background: transparent !important;
          }

          div[data-baseweb="tab-list"] { gap: 0.35rem; }
          div[data-baseweb="tab"] {
            border: 1px solid var(--border);
            border-radius: 11px;
            background: var(--surface-soft);
            padding: 0.42rem 0.78rem;
            color: #313a4f;
            font-weight: 600;
          }
          div[data-baseweb="tab"][aria-selected="true"] {
            background: var(--surface);
            border-color: var(--accent-2);
            color: var(--primary);
          }

          .pipeline {
            margin-top: 0.65rem;
            border: 1px dashed var(--border);
            border-radius: 12px;
            padding: 0.65rem 0.72rem;
            background: var(--surface-soft);
          }

          .pipeline div {
            font-size: 0.84rem;
            color: var(--muted);
            line-height: 1.52;
          }

          .status-banner {
            border: 1px solid var(--border);
            border-radius: 13px;
            background: var(--surface-soft);
            padding: 0.8rem 0.88rem;
            color: #3f4960;
          }

          .status-banner strong {
            color: var(--text);
          }

          .mono-text {
            font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
            font-size: 0.8rem;
          }

          .empty-note {
            border: 1px dashed var(--border);
            border-radius: 12px;
            background: var(--surface);
            padding: 0.9rem;
            color: var(--muted);
            font-size: 0.86rem;
          }

          @keyframes fadeIn {
            from { opacity: 0; transform: translateY(3px); }
            to { opacity: 1; transform: translateY(0px); }
          }

          @media (max-width: 900px) {
            .block-container {
              padding-top: 1.5rem;
            }

            .hero-title {
              font-size: 2rem;
            }
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_header() -> None:
    with st.container():
        st.markdown(
            """
            <div class="hero-shell">
              <h1 class="hero-title">PDF Scraper</h1>
              <p class="hero-copy">Compact pipeline UI for single and batch PDF extraction.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _render_new_run_tab(api_ready: bool) -> None:
    with st.container(border=True):
        with st.form("run_config_form", clear_on_submit=False):
            left, right = st.columns([1.8, 1.2], gap="large", vertical_alignment="top")

            with left:
                st.markdown("<div class='label-title'>Input</div>", unsafe_allow_html=True)
                url_text = st.text_area(
                    "PDF URLs (one per line)",
                    key="url_text",
                    height=185,
                    placeholder="https://example.com/a.pdf\nhttps://example.com/b.pdf",
                    help="One URL per line. Duplicate URLs are removed.",
                )
                upload = st.file_uploader(
                    "Optional URL file (.txt)",
                    type=["txt"],
                    accept_multiple_files=False,
                    key="url_file_upload",
                    help="Comment lines starting with # are ignored.",
                )

                text_urls = parse_urls_from_text(url_text)
                file_urls = parse_urls_from_file_content(upload.getvalue()) if upload else []
                urls = combine_urls(text_urls, file_urls)
                mode = infer_mode(urls)
                mode_for_selection = mode if mode != "empty" else "single"
                allowed_formats = allowed_formats_for_mode(mode_for_selection)

                default_formats = ["markdown", "text"] if mode_for_selection == "single" else ["markdown"]
                selection = [fmt for fmt in st.session_state.selected_formats if fmt in allowed_formats]
                if not selection:
                    selection = [fmt for fmt in default_formats if fmt in allowed_formats]
                if selection != st.session_state.selected_formats:
                    st.session_state.selected_formats = selection

                selected_formats = st.multiselect(
                    "Output formats",
                    options=allowed_formats,
                    key="selected_formats",
                    help="Batch mode supports markdown/html/json. Single mode additionally supports text.",
                )
                if mode_for_selection == "batch":
                    st.caption("`text` is disabled for batch retrieval by design.")

                with st.expander("Optional settings", expanded=False):
                    output_filename = st.text_input(
                        "Output filename (optional)",
                        key="output_filename",
                        placeholder="Auto-generated in output/",
                    )
                    cfg_cols = st.columns(2)
                    with cfg_cols[0]:
                        poll_seconds = st.number_input(
                            "Polling interval (sec)",
                            min_value=1,
                            max_value=60,
                            key="poll_seconds",
                        )
                    with cfg_cols[1]:
                        items_limit = st.number_input(
                            "Batch page size",
                            min_value=1,
                            max_value=200,
                            key="items_limit",
                        )

            with right:
                st.markdown("<div class='label-title'>Run Summary</div>", unsafe_allow_html=True)
                mode_label = "Single" if mode == "single" else "Batch" if mode == "batch" else "Idle"
                mode_tone = "success" if mode != "empty" else "neutral"
                st.markdown(_chip("Mode", mode_label, mode_tone), unsafe_allow_html=True)

                st.markdown("<div style='height:0.55rem'></div>", unsafe_allow_html=True)
                st.markdown(
                    (
                        "<div class='stat-row'>"
                        + _stat_tile("URLs", str(len(urls)), "Unique")
                        + _stat_tile("Formats", str(len(selected_formats)), ", ".join(selected_formats) or "None")
                        + "</div>"
                    ),
                    unsafe_allow_html=True,
                )

                st.markdown(
                    "<div class='pipeline'><div>Single URL: direct scrape.</div><div>Multiple URLs: batch run with progress + item retrieval.</div></div>",
                    unsafe_allow_html=True,
                )

                if not api_ready:
                    st.error("Set `OLOSTEP_API_KEY` in `.env` to run new requests.")

                disable_reasons: List[str] = []
                if not api_ready:
                    disable_reasons.append("Missing API key")
                start_disabled = not api_ready
                start_clicked = st.form_submit_button(
                    "Run Extraction",
                    type="primary",
                    use_container_width=True,
                    disabled=start_disabled,
                )
                if disable_reasons:
                    st.caption("Button disabled: " + " • ".join(disable_reasons))
                elif not urls or not selected_formats:
                    guidance: List[str] = []
                    if not urls:
                        guidance.append("add at least one URL")
                    if not selected_formats:
                        guidance.append("select at least one format")
                    st.caption("Before running: " + " and ".join(guidance) + ".")

    live_box = st.container(border=True)
    result_box = st.container()

    if start_clicked:
        if not urls:
            st.session_state.run_error = "Add at least one URL before starting a run."
        elif not selected_formats:
            st.session_state.run_error = "Select at least one output format before starting a run."
        else:
            request = RunRequest(
                urls=urls,
                formats=list(selected_formats),
                out_filename=(output_filename or "").strip() or None,
                poll_seconds=int(poll_seconds),
                items_limit=int(items_limit),
            )
            _execute_run(request, live_box)

    active_result: Optional[RunResult] = st.session_state.get("active_result")
    if active_result is None:
        with live_box:
            st.markdown("<div class='label-title'>Run Status</div>", unsafe_allow_html=True)
            st.markdown(
                "<div class='empty-note'>No run started yet. Add URL(s) and click <strong>Run Extraction</strong>.</div>",
                unsafe_allow_html=True,
            )
    else:
        with live_box:
            _render_execution_summary(active_result)
        with result_box:
            _render_result_workspace(active_result, origin=st.session_state.get("active_origin") or "latest")

    if st.session_state.get("run_error"):
        st.error(st.session_state["run_error"])


def _execute_run(request: RunRequest, container) -> None:
    with container:
        st.markdown("<div class='label-title'>Run Status</div>", unsafe_allow_html=True)
        status_slot = st.empty()
        progress_slot = st.empty()
        metric_slot = st.empty()
        log_slot = st.empty()

        events: List[RunProgressEvent] = []

        def on_progress(event: RunProgressEvent) -> None:
            events.append(event)
            _render_live_feedback(status_slot, progress_slot, metric_slot, log_slot, events)

        try:
            cfg = load_config()
            result = run_scrape_workflow_sync(request, cfg=cfg, on_progress=on_progress)
            st.session_state.active_result = result
            st.session_state.active_origin = "latest"
            st.session_state.run_error = None
            _render_live_feedback(status_slot, progress_slot, metric_slot, log_slot, result.events)
        except Exception as exc:
            st.session_state.run_error = str(exc)
            st.session_state.active_result = None
            st.session_state.active_origin = None
            status_slot.markdown(
                f"<div class='status-banner'><strong>Run failed.</strong><br/>{exc}</div>",
                unsafe_allow_html=True,
            )
            progress_slot.empty()
            metric_slot.empty()
            log_slot.empty()


def _render_live_feedback(status_slot, progress_slot, metric_slot, log_slot, events: List[RunProgressEvent]) -> None:
    latest = events[-1]
    tone = STATUS_CLASS.get(latest.status or latest.phase, "neutral")
    status_text = STATUS_LABELS.get(latest.status or latest.phase, (latest.status or latest.phase).title())

    status_slot.markdown(
        f"<div class='status-banner'>{_chip('Status', status_text, tone)}<div style='height:0.4rem'></div><strong>{latest.phase.title()}</strong><br/>{latest.message}</div>",
        unsafe_allow_html=True,
    )

    if latest.total > 0:
        progress_slot.progress(min(max(latest.percent, 0.0), 1.0), text=f"{latest.current} / {latest.total}")
    else:
        progress_slot.progress(0.0, text="Preparing")

    with metric_slot.container():
        cols = st.columns(3)
        with cols[0]:
            st.markdown(_stat_tile("Phase", latest.phase.title(), latest.status or "running"), unsafe_allow_html=True)
        with cols[1]:
            st.markdown(_stat_tile("Progress", str(latest.current), f"of {latest.total or 0}"), unsafe_allow_html=True)
        with cols[2]:
            percent = f"{latest.percent * 100:.0f}%" if latest.total else "0%"
            st.markdown(_stat_tile("Completion", percent, "Live"), unsafe_allow_html=True)

    log_rows = [
        {
            "Time": event.timestamp.split("T")[-1],
            "Phase": event.phase,
            "Status": event.status or "",
            "Message": event.message,
        }
        for event in events[-6:]
    ]
    log_slot.dataframe(pd.DataFrame(log_rows), use_container_width=True, hide_index=True)


def _render_execution_summary(result: RunResult) -> None:
    summary = result.summary
    tone = STATUS_CLASS.get(summary.get("status", "empty"), "neutral")
    status = STATUS_LABELS.get(summary.get("status", "empty"), summary.get("status", "Unknown"))

    st.markdown("<div class='label-title'>Run Status</div>", unsafe_allow_html=True)
    st.markdown(
        f"<div class='status-banner'>{_chip('Run', status, tone)}<div style='height:0.42rem'></div><strong>{summary.get('mode', 'unknown').title()} workflow completed.</strong><br/>Output: <span class='mono-text'>{result.output_json}</span></div>",
        unsafe_allow_html=True,
    )

    stats = st.columns(5)
    blocks = [
        ("Mode", summary.get("mode", "-").title(), "Execution path"),
        ("Completed", str(summary.get("completed_count", 0)), "Result rows"),
        ("Failed", str(summary.get("failed_count", 0)), "Failed items"),
        ("Preview", str(summary.get("preview_ready_count", 0)), "Inline/local"),
        ("Artifacts", str(summary.get("artifact_count", 0)), "Generated files"),
    ]
    for col, (label, value, note) in zip(stats, blocks):
        with col:
            st.markdown(_stat_tile(label, value, note), unsafe_allow_html=True)


def _render_result_workspace(result: RunResult, *, origin: str) -> None:
    title = "Current Results" if origin == "latest" else "Loaded Results"
    st.markdown(f"<div class='label-title'>{title}</div>", unsafe_allow_html=True)

    tab_overview, tab_content, tab_artifacts, tab_diagnostics = st.tabs(
        ["Overview", "Content", "Artifacts", "Diagnostics"]
    )
    with tab_overview:
        _render_overview_tab(result)
    with tab_content:
        _render_content_tab(result, origin=origin)
    with tab_artifacts:
        _render_artifacts_tab(result)
    with tab_diagnostics:
        _render_diagnostics_tab(result)


def _render_overview_tab(result: RunResult) -> None:
    rows = []
    for record in result.results:
        source = ", ".join(sorted({x for x in record.content_source.values() if x != "unavailable"})) or "none"
        rows.append(
            {
                "Status": STATUS_LABELS.get(record.status, record.status),
                "Custom ID": record.custom_id,
                "URL": record.url,
                "Retrieve ID": record.retrieve_id or "-",
                "Formats": ", ".join(record.available_formats) or "-",
                "Source": source,
            }
        )

    if not rows:
        st.info("No records to display.")
        return

    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _render_content_tab(result: RunResult, *, origin: str) -> None:
    records = result.results
    if not records:
        st.info("No content to inspect yet.")
        return

    key_base = f"item_{origin}_{abs(hash(result.output_json))}"
    left, right = st.columns([1.05, 2.35], gap="large")

    with left:
        labels = [f"{record.custom_id} | {STATUS_LABELS.get(record.status, record.status)}" for record in records]
        selected = st.radio("Select item", labels, key=key_base)
        record = records[labels.index(selected)]

        st.markdown(_chip("Item", record.custom_id, "neutral"), unsafe_allow_html=True)
        st.caption(record.url or "URL unavailable")
        st.caption(f"Retrieve ID: `{record.retrieve_id or '-'}`")

    with right:
        if record.status == "failed":
            st.error("This item failed during batch processing and has no retrievable preview.")
            with st.expander("Raw failed item"):
                st.json(record.raw_item)
            return

        fmt_tabs = st.tabs([fmt.title() for fmt in FORMAT_ORDER])
        for fmt, tab in zip(FORMAT_ORDER, fmt_tabs):
            with tab:
                _render_preview_panel(result, record, fmt, key_base=key_base)


def _render_preview_panel(result: RunResult, record: ResultRecord, fmt: str, *, key_base: str) -> None:
    preview = resolve_preview_content(
        record,
        fmt,
        hosted_cache=st.session_state.hosted_cache,
        fetch_hosted=False,
    )

    source = preview["source"]
    source_label = source.replace("-", " ").title() if source != "missing" else "Unavailable"
    tone = "success" if source in {"inline", "local", "hosted"} else "warning" if source == "external" else "neutral"
    st.markdown(_chip(fmt.title(), source_label, tone), unsafe_allow_html=True)

    if preview["content"] is None and preview["external_url"]:
        load_key = f"load_{key_base}_{record.custom_id}_{fmt}"
        if st.button(f"Load hosted {fmt} preview", key=load_key):
            preview = resolve_preview_content(
                record,
                fmt,
                hosted_cache=st.session_state.hosted_cache,
                fetch_hosted=True,
            )

    if preview["content"] is None:
        if preview["external_url"]:
            st.info("No local preview found. Fetch hosted content on demand or open the source URL.")
            if preview.get("error"):
                st.caption(preview["error"])
            st.link_button(f"Open hosted {fmt}", preview["external_url"])
            return
        st.info(f"No {fmt} content is available for this item.")
        return

    if preview.get("external_url"):
        st.caption(f"Hosted source: `{preview['external_url']}`")
    elif record.local_files.get(fmt):
        st.caption(f"Local artifact: `{record.local_files[fmt]}`")

    _render_content_blob(preview["content"], fmt)


def _render_content_blob(content: str, fmt: str) -> None:
    if fmt == "json":
        try:
            st.json(json.loads(content))
        except Exception:
            st.code(content, language="json")
        return

    if fmt == "html":
        components.html(content, height=450, scrolling=True)
        with st.expander("HTML source"):
            st.code(content, language="html")
        return

    language = "markdown" if fmt == "markdown" else "text"
    st.code(content, language=language)


def _render_artifacts_tab(result: RunResult) -> None:
    rows = []
    for path in result.artifact_files:
        artifact = Path(path)
        exists = artifact.exists()
        rows.append(
            {
                "Path": str(artifact),
                "Type": "Aggregate JSON" if str(artifact) == result.output_json else artifact.suffix.lstrip(".") or "file",
                "Exists": "Yes" if exists else "No",
                "Size": artifact.stat().st_size if exists else 0,
            }
        )

    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.info("No artifacts found for this run.")


def _render_diagnostics_tab(result: RunResult) -> None:
    with st.expander("Raw payload", expanded=False):
        st.json(result.payload)

    with st.expander("Progress events", expanded=False):
        if result.events:
            rows = [
                {
                    "Time": event.timestamp,
                    "Phase": event.phase,
                    "Status": event.status or "",
                    "Current": event.current,
                    "Total": event.total,
                    "Message": event.message,
                }
                for event in result.events
            ]
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        else:
            st.caption("Saved runs do not include live event streams.")

    with st.expander("Failed items", expanded=False):
        failed_items = result.payload.get("failed_items") or []
        if failed_items:
            st.json(failed_items)
        else:
            st.caption("No failed items for this payload.")


def _render_saved_runs_tab() -> None:
    runs = list_saved_runs(OUTPUT_DIR)
    if not runs:
        st.info("No saved runs found in `output/`.")
        return

    valid_runs = [run for run in runs if not run.get("error")]
    success_runs = [run for run in valid_runs if run.get("status") == "success"]
    partial_runs = [run for run in valid_runs if run.get("status") == "partial"]

    top = st.columns(3)
    with top[0]:
        st.markdown(_stat_tile("Saved runs", str(len(runs)), "JSON outputs"), unsafe_allow_html=True)
    with top[1]:
        st.markdown(_stat_tile("Complete", str(len(success_runs)), "No unresolved items"), unsafe_allow_html=True)
    with top[2]:
        st.markdown(_stat_tile("Partial", str(len(partial_runs)), "Needs review"), unsafe_allow_html=True)

    rows = []
    for run in runs:
        rows.append(
            {
                "Name": run["name"],
                "Mode": run.get("mode", "unknown").title(),
                "Status": STATUS_LABELS.get(run.get("status", "empty"), run.get("status", "Unknown")),
                "Requested": run.get("requested_count", 0),
                "Completed": run.get("completed_count", 0),
                "Failed": run.get("failed_count", 0),
                "Updated": run["modified_at"],
                "Path": run["path"],
            }
        )

    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    options = [run["path"] for run in runs]
    chosen = st.selectbox("Choose a saved run", options=options, format_func=lambda value: Path(value).name)
    if st.button("Open Run", type="primary", use_container_width=False):
        st.session_state.active_result = load_run_result_from_file(chosen)
        st.session_state.active_origin = "saved"
        st.session_state.run_error = None
        st.rerun()

    active_result: Optional[RunResult] = st.session_state.get("active_result")
    if active_result is not None and st.session_state.get("active_origin") == "saved":
        st.markdown("<div style='height:0.65rem'></div>", unsafe_allow_html=True)
        _render_result_workspace(active_result, origin="saved")


def _chip(label: str, value: str, tone: str) -> str:
    return f"<span class='chip {tone}'>{label}: {value}</span>"


def _stat_tile(label: str, value: str, note: str) -> str:
    return (
        "<div class='stat-tile'>"
        f"<div class='stat-key'>{label}</div>"
        f"<div class='stat-value'>{value}</div>"
        f"<div class='stat-note'>{note}</div>"
        "</div>"
    )


if __name__ == "__main__":
    main()
