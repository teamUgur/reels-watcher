"""Local browser dashboard for Instagram Reel discovery and study."""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel, Field


PACKAGE_ROOT = Path(__file__).resolve().parent
STATIC_ROOT = PACKAGE_ROOT / "static"
OUT_ROOT = Path(os.environ.get("REEL_WATCHER_OUT", "reel-watcher-out")).expanduser().resolve()
DB_PATH = OUT_ROOT / "research.sqlite3"
APIFY_ACTOR = "apify~instagram-search-scraper"
APIFY_BASE = "https://api.apify.com/v2"
RUN_COST_CAP_USD = 2.0
DEFAULT_TERMS = ["AI", "AI tools", "ChatGPT", "Claude", "AI agents", "AI automation", "generative AI"]
WINDOWS = {"24h": 1, "7d": 7, "14d": 14, "30d": 30}
CODE_RE = re.compile(r"^[A-Za-z0-9_-]{1,100}$")
_scheduler_thread: threading.Thread | None = None
_scheduler_stop = threading.Event()
_job_lock = threading.Lock()
_worker_pool = None

app = FastAPI(title="Reel Research Desk", docs_url=None, redoc_url=None)


class DiscoverRequest(BaseModel):
    terms: list[str] = Field(default_factory=lambda: list(DEFAULT_TERMS))
    max_results_per_query: int = Field(default=50, ge=1, le=250)


class TrackingRequest(BaseModel):
    enabled: bool
    interval_hours: int = Field(default=6, ge=1, le=168)
    terms: list[str] = Field(default_factory=lambda: list(DEFAULT_TERMS))
    max_results_per_query: int = Field(default=50, ge=1, le=250)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return now_utc().isoformat(timespec="seconds")


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        d = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return d.replace(tzinfo=timezone.utc) if d.tzinfo is None else d.astimezone(timezone.utc)


def configure_out_root(root: Path | str) -> None:
    global OUT_ROOT, DB_PATH
    OUT_ROOT = Path(root).expanduser().resolve()
    DB_PATH = OUT_ROOT / "research.sqlite3"


@contextmanager
def db_connection():
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    con.execute("PRAGMA journal_mode = WAL")
    try:
        yield con
        con.commit()
    finally:
        con.close()


def init_db() -> None:
    with db_connection() as con:
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS reels (
                code TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                caption TEXT NOT NULL DEFAULT '',
                author TEXT,
                published_at TEXT,
                thumbnail_url TEXT,
                video_url TEXT,
                video_file TEXT,
                duration_s REAL,
                views INTEGER,
                likes INTEGER,
                comments INTEGER,
                source_query TEXT,
                source TEXT NOT NULL DEFAULT 'search',
                status TEXT NOT NULL DEFAULT 'discovered',
                analysis_path TEXT,
                keyframe_path TEXT,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL REFERENCES reels(code) ON DELETE CASCADE,
                captured_at TEXT NOT NULL,
                views INTEGER,
                likes INTEGER,
                comments INTEGER
            );
            CREATE INDEX IF NOT EXISTS snapshots_code_time ON snapshots(code, captured_at);
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                job_type TEXT NOT NULL,
                status TEXT NOT NULL,
                stage TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                params_json TEXT NOT NULL DEFAULT '{}',
                result_json TEXT,
                error TEXT
            );
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        columns = {row[1] for row in con.execute("PRAGMA table_info(jobs)")}
        if "params_json" not in columns:
            con.execute("ALTER TABLE jobs ADD COLUMN params_json TEXT NOT NULL DEFAULT '{}' ")
        defaults = {
            "tracking_enabled": "false",
            "tracking_interval_hours": "6",
            "tracking_terms": json.dumps(DEFAULT_TERMS),
            "tracking_max_results": "50",
            "next_scan_at": "",
        }
        for key, value in defaults.items():
            con.execute("INSERT OR IGNORE INTO settings(key, value) VALUES(?, ?)", (key, value))
    import_existing_studies()


def setting(key: str, default: str = "") -> str:
    with db_connection() as con:
        row = con.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_settings(values: dict[str, str]) -> None:
    with db_connection() as con:
        con.executemany(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            list(values.items()),
        )


def safe_code(code: str) -> str:
    if not CODE_RE.fullmatch(code):
        raise HTTPException(status_code=400, detail="Invalid reel code")
    return code


def import_existing_studies() -> None:
    study_dir = OUT_ROOT / "study"
    if not study_dir.exists():
        return
    for path in study_dir.glob("*.json"):
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        code = str(record.get("code") or path.stem)
        if not CODE_RE.fullmatch(code):
            continue
        post = record.get("post") or {}
        url = record.get("url") or ""
        keyframe = record.get("keyframe")
        keyframe_path = str((study_dir / keyframe).resolve()) if keyframe else None
        observed = record.get("analyzed_at") or iso_now()
        with db_connection() as con:
            existing = con.execute("SELECT code FROM reels WHERE code = ?", (code,)).fetchone()
            if existing:
                con.execute(
                    "UPDATE reels SET analysis_path=?, keyframe_path=?, status='ready' WHERE code=?",
                    (str(path.resolve()), keyframe_path, code),
                )
                continue
            con.execute(
                """INSERT INTO reels(code,url,caption,author,published_at,duration_s,views,likes,comments,
                   source_query,source,status,analysis_path,keyframe_path,first_seen,last_seen)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    code, url, str(post.get("caption") or ""), post.get("author"),
                    str(post.get("post_date") or "") or None, post.get("duration_s"),
                    post.get("views"), post.get("likes"), post.get("comments"),
                    "Previously analyzed", "prior_analysis", "ready", str(path.resolve()),
                    keyframe_path, observed, observed,
                ),
            )
            if post.get("views") is not None:
                con.execute(
                    "INSERT INTO snapshots(code,captured_at,views,likes,comments) VALUES(?,?,?,?,?)",
                    (code, observed, post.get("views"), post.get("likes"), post.get("comments")),
                )


def normalize_item(item: dict) -> dict | None:
    url = item.get("url") or item.get("reelUrl") or item.get("permalink") or ""
    short = item.get("shortCode") or item.get("shortcode") or ""
    if not short and url:
        match = re.search(r"/(?:reel|reels|p|tv)/([A-Za-z0-9_-]+)", url)
        short = match.group(1) if match else ""
    if not short:
        return None
    if not url:
        url = f"https://www.instagram.com/reel/{short}/"
    caption = item.get("caption") or item.get("captionText") or ""
    if isinstance(caption, dict):
        caption = caption.get("text") or ""
    user = item.get("ownerUsername") or item.get("username")
    if not user and isinstance(item.get("user"), dict):
        user = item["user"].get("username")
    views = item.get("videoViewCount")
    if views is None:
        views = item.get("videoPlayCount")
    if views is None:
        views = item.get("ig_play_count")
    likes = item.get("likesCount", item.get("like_count", item.get("likes")))
    comments = item.get("commentsCount", item.get("comment_count", item.get("comments")))
    published = item.get("timestamp") or item.get("taken_at_date") or item.get("taken_at")
    return {
        "code": str(short), "url": url, "caption": str(caption)[:4000], "author": user,
        "published_at": str(published) if published else None,
        "thumbnail_url": item.get("displayUrl") or item.get("thumbnail_url") or item.get("thumbnailUrl"),
        "video_url": item.get("videoUrl") or item.get("video_url"),
        "duration_s": item.get("videoDuration") or item.get("video_duration"),
        "views": _int_or_none(views), "likes": _int_or_none(likes), "comments": _int_or_none(comments),
        "source_query": item.get("searchTerm") or item.get("query") or item.get("inputUrl") or "Instagram popular Reels",
    }


def _int_or_none(value):
    try:
        return max(0, int(value)) if value is not None else None
    except (TypeError, ValueError):
        return None


def save_discovery_item(item: dict, captured_at: str) -> None:
    normalized = normalize_item(item)
    if not normalized:
        return
    code = normalized["code"]
    with db_connection() as con:
        prior = con.execute("SELECT status, analysis_path, keyframe_path, video_file FROM reels WHERE code=?", (code,)).fetchone()
        status = prior["status"] if prior and prior["analysis_path"] else "discovered"
        con.execute(
            """INSERT INTO reels(code,url,caption,author,published_at,thumbnail_url,video_url,duration_s,
               views,likes,comments,source_query,source,status,first_seen,last_seen)
               VALUES(:code,:url,:caption,:author,:published_at,:thumbnail_url,:video_url,:duration_s,
               :views,:likes,:comments,:source_query,'search',:status,:captured_at,:captured_at)
               ON CONFLICT(code) DO UPDATE SET url=excluded.url,caption=excluded.caption,
               author=COALESCE(excluded.author,reels.author),published_at=COALESCE(excluded.published_at,reels.published_at),
               thumbnail_url=COALESCE(excluded.thumbnail_url,reels.thumbnail_url),
               video_url=COALESCE(excluded.video_url,reels.video_url),duration_s=COALESCE(excluded.duration_s,reels.duration_s),
               views=COALESCE(excluded.views,reels.views),likes=COALESCE(excluded.likes,reels.likes),
               comments=COALESCE(excluded.comments,reels.comments),source_query=excluded.source_query,
               status=CASE WHEN reels.analysis_path IS NOT NULL THEN 'ready' ELSE 'discovered' END,
               last_seen=excluded.last_seen""",
            {**normalized, "status": status, "captured_at": captured_at},
        )
        con.execute(
            "INSERT INTO snapshots(code,captured_at,views,likes,comments) VALUES(?,?,?,?,?)",
            (code, captured_at, normalized["views"], normalized["likes"], normalized["comments"]),
        )


def performance(views: int | None, likes: int | None) -> tuple[str, list[str]]:
    failures = []
    if views is not None and views < 100_000:
        failures.append("Under 100K views")
    if likes is not None and likes < 5_000:
        failures.append("Under 5K likes")
    if failures:
        return "bad", failures
    if views is None or likes is None:
        return "unavailable", ["Views or likes are unavailable"]
    return "good", []


def growth_for(code: str, days: int, current_views: int | None) -> dict:
    if current_views is None:
        return {"views": None, "ready": False, "observed_hours": None, "baseline_at": None, "note": "View count unavailable"}
    with db_connection() as con:
        rows = con.execute(
            "SELECT captured_at, views FROM snapshots WHERE code=? AND views IS NOT NULL ORDER BY captured_at ASC",
            (code,),
        ).fetchall()
    samples = [(parse_time(row["captured_at"]), row["views"]) for row in rows]
    samples = [(at, views) for at, views in samples if at is not None]
    if len(samples) < 2:
        return {"views": None, "ready": False, "observed_hours": None, "baseline_at": None, "note": "Collecting baseline"}
    current_at = samples[-1][0]
    target = current_at - timedelta(days=days)
    baseline_at, baseline_views = min(samples[:-1], key=lambda pair: abs((pair[0] - target).total_seconds()))
    observed_hours = max(0.0, (current_at - baseline_at).total_seconds() / 3600)
    tolerance_hours = max(4.0, days * 24 * 0.15)
    if abs((baseline_at - target).total_seconds()) / 3600 > tolerance_hours:
        return {
            "views": None, "ready": False, "observed_hours": round(observed_hours, 1),
            "baseline_at": baseline_at.isoformat(timespec="seconds"), "note": "Waiting for a snapshot near this window",
        }
    return {
        "views": int(current_views - baseline_views), "ready": True,
        "observed_hours": round(observed_hours, 1), "baseline_at": baseline_at.isoformat(timespec="seconds"),
        "note": "Observed from saved snapshots",
    }


def reel_summary(row: sqlite3.Row, window: str) -> dict:
    result = dict(row)
    status, reasons = performance(result.get("views"), result.get("likes"))
    result["performance"] = status
    result["performance_reasons"] = reasons
    days = WINDOWS[window]
    result["growth"] = growth_for(result["code"], days, result.get("views"))
    result["analysis_available"] = bool(result.get("analysis_path"))
    if result.get("keyframe_path"):
        result["keyframe_url"] = f"/files/study/{result['code']}.jpg"
    elif result.get("thumbnail_url"):
        result["keyframe_url"] = result["thumbnail_url"]
    if result.get("video_file"):
        result["video_url_local"] = f"/files/media/{result['code']}.mp4"
    result.pop("analysis_path", None)
    result.pop("keyframe_path", None)
    result.pop("video_url", None)
    return result


def create_job(job_type: str, stage: str, message: str, params: dict | None = None) -> str:
    job_id = uuid.uuid4().hex[:12]
    current = iso_now()
    with db_connection() as con:
        con.execute(
            "INSERT INTO jobs(id,job_type,status,stage,message,created_at,updated_at,params_json,result_json) VALUES(?,?,?,?,?,?,?,?,?)",
            (job_id, job_type, "queued", stage, message, current, current, json.dumps(params or {}), "{}"),
        )
    return job_id


def update_job(job_id: str, *, status: str | None = None, stage: str | None = None,
               message: str | None = None, result: dict | None = None, error: str | None = None) -> None:
    fields = ["updated_at=?"]
    values: list[object] = [iso_now()]
    for column, value in (("status", status), ("stage", stage), ("message", message), ("error", error)):
        if value is not None:
            fields.append(f"{column}=?")
            values.append(value)
    if result is not None:
        fields.append("result_json=?")
        values.append(json.dumps(result, ensure_ascii=False))
    values.append(job_id)
    with db_connection() as con:
        con.execute(f"UPDATE jobs SET {', '.join(fields)} WHERE id=?", values)


def get_job(job_id: str) -> dict | None:
    with db_connection() as con:
        row = con.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    return _job_dict(row) if row else None


def _job_dict(row: sqlite3.Row) -> dict:
    result = dict(row)
    try:
        result["params"] = json.loads(result.pop("params_json") or "{}")
    except json.JSONDecodeError:
        result["params"] = {}
        result.pop("params_json", None)
    try:
        result["result"] = json.loads(result.pop("result_json") or "{}")
    except json.JSONDecodeError:
        result["result"] = {}
        result.pop("result_json", None)
    return result


def apify_request(method: str, url: str, body: dict | None = None, timeout: int = 60):
    token = os.environ.get("APIFY_TOKEN", "").strip()
    if not token:
        raise RuntimeError("APIFY_TOKEN is not set in the process environment")
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}", "User-Agent": "reel-watcher-dashboard"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8") or "null")
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode("utf-8", errors="replace")[:1200]
        raise RuntimeError(f"Apify returned HTTP {exc.code}: {body_text}") from None


def clean_terms(terms: list[str]) -> list[str]:
    cleaned = []
    seen = set()
    for term in terms:
        value = re.sub(r"\s+", " ", str(term)).strip()
        if value and value.casefold() not in seen:
            seen.add(value.casefold())
            cleaned.append(value[:80])
    if not cleaned:
        raise HTTPException(status_code=422, detail="Add at least one search term")
    if len(cleaned) > 12:
        raise HTTPException(status_code=422, detail="Use no more than 12 search terms per scan")
    return cleaned


def has_running_discovery() -> bool:
    with db_connection() as con:
        row = con.execute("SELECT 1 FROM jobs WHERE job_type='discover' AND status IN ('queued','running') LIMIT 1").fetchone()
    return bool(row)


def start_discovery(terms: list[str], max_results: int, initiated_by: str) -> str:
    if has_running_discovery():
        raise HTTPException(status_code=409, detail="A topic scan is already running")
    terms = clean_terms(terms)
    job_id = create_job("discover", "connecting", "Starting Instagram topic search", {"terms": terms, "source": initiated_by})
    update_job(job_id, status="running")
    if initiated_by == "auto":
        interval = int(setting("tracking_interval_hours", "6"))
        set_settings({"next_scan_at": (now_utc() + timedelta(hours=interval)).isoformat(timespec="seconds")})
    _worker_pool.submit(run_discovery, job_id, terms, max_results)
    return job_id


def run_discovery(job_id: str, terms: list[str], max_results: int) -> None:
    started = iso_now()
    try:
        update_job(job_id, stage="searching", message=f"Searching {len(terms)} topic phrases on Instagram")
        encoded_cap = urllib.parse.urlencode({"maxTotalChargeUsd": f"{RUN_COST_CAP_USD:.2f}"})
        run_response = apify_request(
            "POST", f"{APIFY_BASE}/acts/{APIFY_ACTOR}/runs?{encoded_cap}",
            {
                "search": ", ".join(terms), "searchType": "popular", "searchLimit": max_results,
                "includePopularReelMedia": True, "maxConcurrency": 3,
            }, timeout=120,
        )
        run = run_response.get("data") or {}
        run_id = run.get("id")
        if not run_id:
            raise RuntimeError("Apify did not return a run ID")
        update_job(job_id, stage="collecting", message="Apify is collecting public Reel results")
        deadline = time.time() + 900
        while run.get("status") in ("READY", "RUNNING"):
            if time.time() > deadline:
                raise RuntimeError("Apify scan exceeded the 15 minute limit")
            time.sleep(3)
            run_response = apify_request("GET", f"{APIFY_BASE}/actor-runs/{run_id}", timeout=30)
            run = run_response.get("data") or {}
            if run.get("status") == "RUNNING":
                update_job(job_id, stage="collecting", message="Collecting Reels and current engagement totals")
        if run.get("status") != "SUCCEEDED":
            raise RuntimeError(f"Apify run ended {run.get('status')}: {run.get('statusMessage') or 'no details'}")
        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            raise RuntimeError("Apify run did not return a result dataset")
        items = apify_request(
            "GET", f"{APIFY_BASE}/datasets/{dataset_id}/items?clean=true&format=json", timeout=120,
        ) or []
        captured = iso_now()
        update_job(job_id, stage="saving", message=f"Saving {len(items)} results and view snapshots")
        before = set()
        with db_connection() as con:
            before = {row[0] for row in con.execute("SELECT code FROM reels")}
        for item in items:
            if isinstance(item, dict):
                save_discovery_item(item, captured)
        with db_connection() as con:
            after = con.execute("SELECT COUNT(*) FROM reels").fetchone()[0]
        actual_cost = run.get("usageTotalUsd")
        cost_entry = {"at": captured, "run_id": run_id, "items": len(items), "usd": actual_cost,
                      "status": run.get("status"), "terms": terms, "cap_usd": RUN_COST_CAP_USD}
        OUT_ROOT.mkdir(parents=True, exist_ok=True)
        with (OUT_ROOT / "apify_cost.jsonl").open("a", encoding="utf-8") as log:
            log.write(json.dumps(cost_entry, ensure_ascii=False) + "\n")
        update_job(job_id, status="complete", stage="complete", message="Topic scan complete", result={
            "run_id": run_id, "items_received": len(items), "new_reels": max(0, after - len(before)),
            "total_reels": after, "cost_usd": actual_cost, "captured_at": captured,
        })
    except Exception as exc:
        update_job(job_id, status="error", stage="error", message="Topic scan failed", error=str(exc)[:1500])


def media_host_allowed(url: str) -> bool:
    parsed = urllib.parse.urlparse(url)
    host = (parsed.hostname or "").lower()
    return parsed.scheme == "https" and (host.endswith("cdninstagram.com") or host.endswith("fbcdn.net"))


def download_reel(url: str, destination: Path, job_id: str) -> None:
    if not media_host_allowed(url):
        raise RuntimeError("The stored media link is not an Instagram CDN URL. Refresh the topic search and try again.")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_suffix(".part")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.instagram.com/"})
    max_bytes = 300 * 1024 * 1024
    with urllib.request.urlopen(req, timeout=90) as response, temp.open("wb") as file:
        length = response.headers.get("Content-Length")
        if length and int(length) > max_bytes:
            raise RuntimeError("Reel video is larger than the 300 MB local limit")
        total = 0
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                raise RuntimeError("Reel video is larger than the 300 MB local limit")
            file.write(chunk)
    temp.replace(destination)
    update_job(job_id, stage="downloaded", message="Video saved locally")


def run_analysis(job_id: str, code: str) -> None:
    try:
        with db_connection() as con:
            reel = con.execute("SELECT * FROM reels WHERE code=?", (code,)).fetchone()
        if not reel:
            raise RuntimeError("Reel was not found")
        existing_video = Path(reel["video_file"]) if reel["video_file"] else None
        if not reel["video_url"] and not (existing_video and existing_video.is_file()):
            raise RuntimeError("No downloadable video link is available. Refresh the topic search to renew it.")
        video_file = existing_video if existing_video and existing_video.is_file() else OUT_ROOT / "media" / f"{code}.mp4"
        if not video_file.exists():
            update_job(job_id, stage="downloading", message="Downloading the Reel to this Mac")
            download_reel(reel["video_url"], video_file, job_id)
        metadata = {
            "caption": reel["caption"], "author": reel["author"], "author_id": None,
            "likes": reel["likes"], "views": reel["views"], "comments": reel["comments"],
            "post_date": reel["published_at"], "duration_s": reel["duration_s"], "music_info": None,
        }
        meta_dir = OUT_ROOT / ".metadata"
        meta_dir.mkdir(parents=True, exist_ok=True)
        meta_path = meta_dir / f"{code}.json"
        meta_path.write_text(json.dumps({code: metadata}, ensure_ascii=False), encoding="utf-8")
        progress_path = OUT_ROOT / ".jobs" / f"{job_id}.progress.json"
        progress_path.parent.mkdir(parents=True, exist_ok=True)
        update_job(job_id, stage="local_analysis", message="Loading local vision model and reading frames")
        env = os.environ.copy()
        env.pop("APIFY_TOKEN", None)
        env["REEL_WATCHER_PROGRESS_FILE"] = str(progress_path)
        command = [
            sys.executable, "-m", "reel_watcher.cli", "study", "--local", str(video_file),
            "--local-metadata", str(meta_path), "--out-root", str(OUT_ROOT),
            "--archive", str(OUT_ROOT / "archive"),
        ]
        log_dir = OUT_ROOT / ".jobs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"{job_id}.log"
        with log_path.open("w", encoding="utf-8") as log:
            process = subprocess.Popen(command, cwd=str(Path.cwd()), env=env, stdout=log, stderr=subprocess.STDOUT)
            previous_progress = None
            while process.poll() is None:
                if progress_path.exists():
                    try:
                        progress = json.loads(progress_path.read_text(encoding="utf-8"))
                    except (OSError, json.JSONDecodeError):
                        progress = None
                    if progress and progress != previous_progress:
                        update_job(job_id, stage=progress.get("stage", "local_analysis"), message=progress.get("message", "Analyzing locally"))
                        previous_progress = progress
                time.sleep(0.6)
            return_code = process.wait()
        if return_code != 0:
            tail = log_path.read_text(encoding="utf-8", errors="replace")[-1500:]
            raise RuntimeError(tail.strip() or f"Local analysis exited with code {return_code}")
        analysis_path = OUT_ROOT / "study" / f"{code}.json"
        if not analysis_path.exists():
            raise RuntimeError("Analysis ended without writing a study JSON")
        keyframe_path = OUT_ROOT / "study" / f"{code}.jpg"
        with db_connection() as con:
            con.execute(
                "UPDATE reels SET status='ready',analysis_path=?,keyframe_path=?,video_file=? WHERE code=?",
                (str(analysis_path.resolve()), str(keyframe_path.resolve()) if keyframe_path.exists() else None,
                 str(video_file.resolve()), code),
            )
        update_job(job_id, status="complete", stage="complete", message="Reel study is ready", result={"code": code})
    except Exception as exc:
        with db_connection() as con:
            con.execute("UPDATE reels SET status='error' WHERE code=? AND status!='ready'", (code,))
        update_job(job_id, status="error", stage="error", message="Reel analysis failed", error=str(exc)[:1500])


def get_all_reels() -> list[sqlite3.Row]:
    with db_connection() as con:
        return con.execute("SELECT * FROM reels ORDER BY last_seen DESC, code ASC").fetchall()


def list_jobs(limit: int = 10) -> list[dict]:
    with db_connection() as con:
        rows = con.execute("SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
    return [_job_dict(row) for row in rows]


def scan_scheduler() -> None:
    while not _scheduler_stop.wait(15):
        if setting("tracking_enabled", "false") != "true":
            continue
        if has_running_discovery():
            continue
        due = parse_time(setting("next_scan_at"))
        if due and due <= now_utc():
            try:
                terms = json.loads(setting("tracking_terms", json.dumps(DEFAULT_TERMS)))
                max_results = int(setting("tracking_max_results", "50"))
                start_discovery(terms, max_results, "auto")
            except Exception:
                interval = int(setting("tracking_interval_hours", "6"))
                set_settings({"next_scan_at": (now_utc() + timedelta(hours=interval)).isoformat(timespec="seconds")})


def start_scheduler() -> None:
    global _scheduler_thread, _worker_pool
    if _worker_pool is None:
        from concurrent.futures import ThreadPoolExecutor
        _worker_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="reel-watcher")
    if _scheduler_thread is None or not _scheduler_thread.is_alive():
        _scheduler_stop.clear()
        _scheduler_thread = threading.Thread(target=scan_scheduler, name="reel-watcher-scheduler", daemon=True)
        _scheduler_thread.start()


@app.get("/", response_class=HTMLResponse)
def index():
    page = STATIC_ROOT / "dashboard.html"
    if not page.exists():
        raise HTTPException(status_code=500, detail="Dashboard UI file is missing")
    return HTMLResponse(page.read_text(encoding="utf-8"))


@app.get("/api/health")
def health():
    return {"ok": True, "apify_configured": bool(os.environ.get("APIFY_TOKEN", "").strip())}


@app.get("/api/state")
def state(window: str = "24h"):
    if window not in WINDOWS:
        raise HTTPException(status_code=422, detail="Choose one of 24h, 7d, 14d, or 30d")
    reels = [reel_summary(row, window) for row in get_all_reels()]
    reels.sort(key=lambda item: (
        item["growth"]["views"] is not None,
        item["growth"]["views"] if item["growth"]["views"] is not None else -1,
        item.get("views") or -1,
    ), reverse=True)
    counts = {"total": len(reels), "good": 0, "bad": 0, "unavailable": 0, "analyzed": 0}
    for reel in reels:
        counts[reel["performance"]] = counts.get(reel["performance"], 0) + 1
        if reel["analysis_available"]:
            counts["analyzed"] += 1
    return {
        "reels": reels, "counts": counts, "jobs": list_jobs(),
        "apify_configured": bool(os.environ.get("APIFY_TOKEN", "").strip()),
        "tracking": {
            "enabled": setting("tracking_enabled", "false") == "true",
            "interval_hours": int(setting("tracking_interval_hours", "6")),
            "next_scan_at": setting("next_scan_at"),
            "terms": json.loads(setting("tracking_terms", json.dumps(DEFAULT_TERMS))),
        },
        "window": window, "cost_cap_usd": RUN_COST_CAP_USD,
    }


@app.get("/api/reels/{code}")
def reel_detail(code: str, window: str = "24h"):
    code = safe_code(code)
    if window not in WINDOWS:
        raise HTTPException(status_code=422, detail="Choose one of 24h, 7d, 14d, or 30d")
    with db_connection() as con:
        row = con.execute("SELECT * FROM reels WHERE code=?", (code,)).fetchone()
        snapshots = con.execute(
            "SELECT captured_at,views,likes,comments FROM snapshots WHERE code=? ORDER BY captured_at ASC", (code,),
        ).fetchall()
    if not row:
        raise HTTPException(status_code=404, detail="Reel not found")
    result = reel_summary(row, window)
    analysis_path = Path(row["analysis_path"]) if row["analysis_path"] else None
    analysis = None
    if analysis_path and analysis_path.exists():
        try:
            analysis = json.loads(analysis_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            analysis = None
    result["analysis"] = analysis
    result["frames"] = []
    archive_dir = Path((analysis or {}).get("archive") or "") if (analysis or {}).get("archive") else None
    frames = (analysis or {}).get("frames") or []
    if archive_dir:
        for frame in frames:
            rel = frame.get("file")
            candidate = (archive_dir / rel).resolve() if rel else None
            if candidate and candidate.is_relative_to(archive_dir.resolve()) and candidate.is_file():
                frame["image_url"] = f"/files/archive/{code}/{rel}"
    result["frames"] = frames
    result["frames"] = frames
    result["snapshots"] = [dict(s) for s in snapshots]
    result["transcript"] = (analysis or {}).get("transcript") or {"text": "", "words": []}
    result["analysis_sections"] = (analysis or {}).get("analysis") or {}
    return result


@app.get("/api/jobs")
def jobs():
    return {"jobs": list_jobs(20)}


@app.post("/api/discover")
def discover(request: DiscoverRequest):
    if not os.environ.get("APIFY_TOKEN", "").strip():
        raise HTTPException(status_code=503, detail="Set APIFY_TOKEN in the Terminal session before starting the dashboard")
    start_scheduler()
    job_id = start_discovery(request.terms, request.max_results_per_query, "manual")
    return {"job_id": job_id, "message": "Topic scan queued", "cost_cap_usd": RUN_COST_CAP_USD}


@app.post("/api/tracking")
def tracking(request: TrackingRequest):
    if request.enabled and not os.environ.get("APIFY_TOKEN", "").strip():
        raise HTTPException(status_code=503, detail="Set APIFY_TOKEN in the Terminal session before enabling tracking")
    terms = clean_terms(request.terms)
    next_scan = ""
    if request.enabled:
        next_scan = (now_utc() + timedelta(hours=request.interval_hours)).isoformat(timespec="seconds")
    set_settings({
        "tracking_enabled": "true" if request.enabled else "false",
        "tracking_interval_hours": str(request.interval_hours),
        "tracking_terms": json.dumps(terms, ensure_ascii=False),
        "tracking_max_results": str(request.max_results_per_query),
        "next_scan_at": next_scan,
    })
    start_scheduler()
    return {"enabled": request.enabled, "interval_hours": request.interval_hours, "next_scan_at": next_scan}


@app.post("/api/reels/{code}/analyze")
def analyze_reel(code: str):
    code = safe_code(code)
    with db_connection() as con:
        row = con.execute("SELECT status,analysis_path,video_file,video_url FROM reels WHERE code=?", (code,)).fetchone()
        active = con.execute(
            "SELECT id FROM jobs WHERE job_type='analyze' AND status IN ('queued','running') AND params_json LIKE ? LIMIT 1",
            (f'%"code": "{code}"%',),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Reel not found")
    if row["analysis_path"] and Path(row["analysis_path"]).exists():
        return {"already_analyzed": True, "code": code}
    if active:
        return {"job_id": active["id"], "already_running": True}
    if not row["video_url"] and not row["video_file"]:
        raise HTTPException(status_code=422, detail="No Reel video link is available. Refresh the topic search first.")
    if row["video_file"] and Path(row["video_file"]).exists():
        video_path = Path(row["video_file"])
    else:
        video_path = None
    job_id = create_job("analyze", "queued", "Preparing local Reel analysis", {"code": code})
    with db_connection() as con:
        con.execute("UPDATE reels SET status='analyzing' WHERE code=?", (code,))
    start_scheduler()
    _worker_pool.submit(run_analysis, job_id, code)
    return {"job_id": job_id, "code": code, "message": "Local analysis queued"}


@app.get("/files/{relative_path:path}")
def output_file(relative_path: str):
    root = OUT_ROOT.resolve()
    target = (root / relative_path).resolve()
    if not target.is_relative_to(root) or not target.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(target)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="reel-watcher dashboard", description="Run the local Reel Research Desk")
    parser.add_argument("--host", default="127.0.0.1", help="bind address; keep this local")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--out-root", default="./reel-watcher-out")
    args = parser.parse_args(argv)
    if args.host not in ("127.0.0.1", "localhost"):
        parser.error("The dashboard only supports localhost")
    configure_out_root(args.out_root)
    init_db()
    start_scheduler()
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")
    return 0

