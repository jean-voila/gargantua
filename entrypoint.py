#!/usr/bin/env python3
"""Gargantua entrypoint: Soulseek bulk downloader on top of sldl with a rich TUI."""
from __future__ import annotations

import json
import os
import re
import secrets
import shlex
import signal
import string
import subprocess
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlparse
from urllib.request import Request, urlopen

from rich.align import Align
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table
from rich.text import Text

DATA_DIR = Path("/data")
DOWNLOADS_DIR = Path("/downloads")
LOG_FILE = DATA_DIR / "sldl.log"
INDEX_PATH = DATA_DIR / "index.sldl"
CREDENTIALS_FILE = DATA_DIR / "credentials.txt"
MB_CACHE_FILE = DATA_DIR / "mb_cache.json"

MB_API = "https://musicbrainz.org/ws/2/recording/"
MB_USER_AGENT = "gargantua/1.0 ( https://github.com/jean-voila/gargantua )"
MB_RATE_LIMIT_SECONDS = 1.0
MB_DEFAULT_MIN_SCORE = 85

# Tags routinely appended to YouTube video titles that have nothing to do with
# the actual recording — they break exact-string searches on Soulseek (e.g.
# "Supertramp - Gone Hollywood (Official Audio)" yields zero hits because no
# user names their FLAC files that way).
_NOISE_PATTERNS = [
    r"official\s*(?:music\s*)?(?:audio|video|lyric\s*video|lyrics?\s*video)",
    r"official",
    r"lyric\s*video",
    r"lyrics(?:\s*video)?",
    r"audio(?:\s*only)?",
    r"video",
    r"music\s*video",
    r"hd|hq|4k|1080p?|720p?|480p?",
    r"remastered(?:\s*\d{4})?",
    r"\d{4}\s*remaster(?:ed)?",
    r"full\s*album",
    r"visualizer",
    r"clip\s*officiel",
    r"audio\s*officiel",
    r"vid[eé]o\s*officielle",
]
NOISE_RE = re.compile(
    r"\s*[\(\[]\s*(?:" + "|".join(_NOISE_PATTERNS) + r")\s*[\)\]]",
    re.IGNORECASE,
)

ASCII_BANNER = r"""
   ____                              _
  / ___| __ _ _ __ __ _  __ _ _ __ | |_ _   _  __ _
 | |  _ / _` | '__/ _` |/ _` | '_ \| __| | | |/ _` |
 | |_| | (_| | | | (_| | (_| | | | | |_| |_| | (_| |
  \____|\__,_|_|  \__, |\__,_|_| |_|\__|\__,_|\__,_|
                  |___/
"""

console = Console(force_terminal=True, color_system="truecolor")


# ---------- Configuration ----------------------------------------------------

@dataclass
class Config:
    username: str
    password: str
    generated_credentials: bool
    input_source: str
    input_label: str
    pref_format: str
    name_format: str
    concurrent_downloads: int
    strict_conditions: bool
    fast_search: bool
    write_playlist: bool
    spotify_id: str
    spotify_secret: str
    input_type: str | None = None
    extra_args: list[str] = field(default_factory=list)
    youtube_origin: bool = False
    minimal: bool = False


def env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "y"}


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        console.print(f"[yellow]warning:[/] {name}={raw!r} is not an int, using default {default}")
        return default


def random_username() -> str:
    suffix = secrets.token_hex(4)
    return f"gargantua-{suffix}"


def random_password(length: int = 24) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def _find_in_data(env_name: str, suffix: str, preferred: str) -> Path | None:
    name = (os.environ.get(env_name) or "").strip()
    if name:
        candidate = DATA_DIR / name
        if not candidate.is_file():
            console.print(f"[red]error:[/] {env_name} file [bold]{candidate}[/] not found.")
            sys.exit(1)
        return candidate
    pref = DATA_DIR / preferred
    if pref.is_file():
        return pref
    matches = sorted(DATA_DIR.glob(f"*{suffix}"))
    return matches[0] if matches else None


_URL_SHELL_ESCAPE_RE = re.compile(r"\\([?=&#!*'()\[\] ])")


def _clean_url(url: str) -> str:
    """Strip shell-escape backslashes (e.g. ``\\?``, ``\\=``) that users sometimes
    leave inside double-quoted URLs when copying from a zsh-style command line.
    Raw backslashes aren't valid in URLs, so this is a safe normalization."""
    cleaned = _URL_SHELL_ESCAPE_RE.sub(r"\1", url)
    if cleaned != url:
        console.print(
            f"[yellow]warning:[/] stripped shell-escape backslashes from URL "
            f"(received {url!r}, using {cleaned!r})."
        )
    return cleaned


def _label_for_url(url: str) -> str:
    lower = url.lower()
    if "spotify.com" in lower:
        return f"Spotify playlist ({url})"
    if "youtube.com" in lower or "youtu.be" in lower:
        return f"YouTube playlist ({url})"
    if "bandcamp.com" in lower:
        return f"Bandcamp ({url})"
    if "musicbrainz.org" in lower:
        return f"MusicBrainz ({url})"
    return f"URL ({url})"


def _youtube_video_id(url: str) -> str | None:
    """Return the YouTube video ID if ``url`` points to a single video (``watch``,
    ``shorts``, or ``youtu.be``) and is *not* a playlist (``list=``). Returns None
    for playlists or non-YouTube URLs."""
    try:
        parsed = urlparse(url)
    except ValueError:
        return None
    host = parsed.netloc.lower()
    if "youtube.com" not in host and "youtu.be" not in host:
        return None
    qs = parse_qs(parsed.query)
    if qs.get("list"):
        return None
    if "youtu.be" in host:
        vid = parsed.path.lstrip("/").split("/", 1)[0]
        return vid or None
    if parsed.path == "/watch":
        return (qs.get("v") or [None])[0]
    if parsed.path.startswith("/shorts/"):
        parts = parsed.path.split("/")
        return parts[2] if len(parts) >= 3 and parts[2] else None
    return None


def _yt_dlp_title(video_id: str) -> str:
    """Fetch the title of a YouTube video via yt-dlp. Exits the process on
    failure with a clear message, since without a title we have nothing to
    search for."""
    url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        result = subprocess.run(
            ["yt-dlp", "--skip-download", "--no-warnings", "--print", "title", url],
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
    except FileNotFoundError:
        console.print("[red]error:[/] yt-dlp is not installed in the container.")
        sys.exit(1)
    except subprocess.TimeoutExpired:
        console.print(f"[red]error:[/] yt-dlp timed out fetching metadata for {url}.")
        sys.exit(1)
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        console.print(f"[red]error:[/] yt-dlp failed to fetch metadata for {url}.")
        if stderr:
            console.print(Text(stderr, style="dim"))
        sys.exit(1)
    title = result.stdout.strip().splitlines()[0].strip() if result.stdout else ""
    if not title:
        console.print(f"[red]error:[/] yt-dlp returned an empty title for {url}.")
        sys.exit(1)
    return title


def _yt_dlp_playlist_titles(url: str) -> list[str]:
    """Return the list of video titles in a YouTube playlist via yt-dlp.
    Uses ``--flat-playlist`` so we don't fetch each video's full metadata."""
    try:
        result = subprocess.run(
            [
                "yt-dlp", "--flat-playlist", "--no-warnings",
                "--print", "%(title)s", url,
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=180,
        )
    except FileNotFoundError:
        console.print("[red]error:[/] yt-dlp is not installed in the container.")
        sys.exit(1)
    except subprocess.TimeoutExpired:
        console.print(f"[red]error:[/] yt-dlp timed out fetching playlist {url}.")
        sys.exit(1)
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        console.print(f"[red]error:[/] yt-dlp failed to fetch playlist {url}.")
        if stderr:
            console.print(Text(stderr, style="dim"))
        sys.exit(1)
    titles = [t.strip() for t in (result.stdout or "").splitlines() if t.strip()]
    if not titles:
        console.print(f"[red]error:[/] no titles found in playlist {url}.")
        sys.exit(1)
    return titles


def _is_youtube_playlist(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    host = parsed.netloc.lower()
    if "youtube.com" not in host and "youtu.be" not in host:
        return False
    qs = parse_qs(parsed.query)
    return bool(qs.get("list")) or parsed.path == "/playlist"


def _clean_youtube_title(title: str) -> str:
    """Strip the noise tags YouTube uploaders append to titles
    (``(Official Audio)``, ``[Lyrics]``, ``(Remastered 2009)`` …) so the result
    is usable as a Soulseek search query. Only touches content inside ``()`` /
    ``[]`` to avoid mangling actual words in the title."""
    cleaned = NOISE_RE.sub("", title)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -–—")
    return cleaned or title.strip()


# ---------- MusicBrainz lookup ----------------------------------------------

_mb_last_request: float = 0.0


def _load_mb_cache() -> dict[str, list[str]]:
    """Load the MusicBrainz cache from /data. Hits only — misses are not
    persisted so MB additions become visible on the next run."""
    if not MB_CACHE_FILE.is_file():
        return {}
    try:
        raw = json.loads(MB_CACHE_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    if not isinstance(raw, dict):
        return {}
    return {k: list(v) for k, v in raw.items() if isinstance(v, list) and len(v) == 2}


def _save_mb_cache(cache: dict[str, list[str]]) -> None:
    try:
        MB_CACHE_FILE.write_text(
            json.dumps(cache, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
    except OSError:
        pass


_LUCENE_SPECIAL_RE = re.compile(r'([+\-!(){}\[\]^"~*?:\\/]|&&|\|\|)')


def _lucene_escape(s: str) -> str:
    """Escape characters that Lucene treats as operators in MB queries."""
    return _LUCENE_SPECIAL_RE.sub(r"\\\1", s)


def _split_artist_title(cleaned: str) -> tuple[str | None, str | None]:
    """Split a cleaned YouTube title on the first `` - `` (or its em/en-dash
    variants) into ``(artist, title)``. Returns ``(None, None)`` if no
    separator is present."""
    for sep in (" - ", " – ", " — "):
        if sep in cleaned:
            artist, _, title = cleaned.partition(sep)
            artist = artist.strip()
            title = title.strip()
            if artist and title:
                return artist, title
            return None, None
    return None, None


def _credit_to_artist(credits: list[dict]) -> str:
    return "".join(
        (c.get("name") or (c.get("artist") or {}).get("name") or "")
        + (c.get("joinphrase") or "")
        for c in credits
    ).strip()


def _mb_query(raw_title: str, min_score: int) -> tuple[str, str] | None:
    """Query MusicBrainz for the recording best matching ``raw_title``. Returns
    ``(artist, title)`` if a confident hit is found, else None.

    YouTube titles of the form ``Artist - Title`` are queried with a structured
    Lucene query (``recording:"…" AND artist:"…"``) so MB doesn't return
    covers/tributes ranked 100 because the title alone matches. Without an
    artist hint we skip the lookup entirely — top-scored unstructured matches
    are too unreliable to feed into Soulseek search.

    Enforces the 1 req/s anonymous rate limit. Network/parse failures return
    None so the caller can fall back to the cleaned YouTube title."""
    cleaned = _clean_youtube_title(raw_title)
    artist_hint, title_hint = _split_artist_title(cleaned)
    if not artist_hint or not title_hint:
        return None

    global _mb_last_request
    elapsed = time.monotonic() - _mb_last_request
    if elapsed < MB_RATE_LIMIT_SECONDS:
        time.sleep(MB_RATE_LIMIT_SECONDS - elapsed)

    lucene = (
        f'recording:"{_lucene_escape(title_hint)}"'
        f' AND artist:"{_lucene_escape(artist_hint)}"'
    )
    url = f"{MB_API}?query={quote(lucene)}&fmt=json&limit=10"
    req = Request(url, headers={"User-Agent": MB_USER_AGENT, "Accept": "application/json"})
    try:
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, OSError):
        return None
    finally:
        _mb_last_request = time.monotonic()

    recordings = data.get("recordings") or []
    if not recordings:
        return None

    # Re-rank: real artist match wins over score, then score breaks ties. This
    # handles the "Logical Song [Supertramp]" cover case — MB ranks the cover
    # at 100, but its artist field doesn't contain "Supertramp", so it loses.
    artist_hint_low = artist_hint.lower()

    def _rank(rec: dict) -> tuple[int, int]:
        score = int(rec.get("score") or 0)
        artist = _credit_to_artist(rec.get("artist-credit") or []).lower()
        return (1 if artist_hint_low in artist else 0, score)

    recordings.sort(key=_rank, reverse=True)
    best = recordings[0]
    score = int(best.get("score") or 0)
    if score < min_score:
        return None
    title = (best.get("title") or "").strip()
    credits = best.get("artist-credit") or []
    if not title or not credits:
        return None
    artist = _credit_to_artist(credits)
    if not artist:
        return None
    # Final guard: if the chosen artist doesn't contain the hint at all, the
    # match is a tribute / cover / unrelated recording — drop it.
    if artist_hint_low not in artist.lower():
        return None
    return artist, title


def _resolve_title(
    raw_title: str,
    cache: dict[str, list[str]],
    min_score: int,
    enable_mb: bool,
) -> tuple[str, str]:
    """Return ``(query, source)`` for ``raw_title``. ``source`` is either
    ``"musicbrainz"`` (canonical artist - title) or ``"cleaned"`` (YouTube
    title with noise tags stripped). Cache hits never re-query MB."""
    if enable_mb:
        hit = cache.get(raw_title)
        if hit is not None:
            return f"{hit[0]} - {hit[1]}", "musicbrainz"
        result = _mb_query(raw_title, min_score)
        if result is not None:
            cache[raw_title] = [result[0], result[1]]
            return f"{result[0]} - {result[1]}", "musicbrainz"
    return _clean_youtube_title(raw_title), "cleaned"


def _build_resolved_list(titles: list[str]) -> tuple[Path, int, int]:
    """Resolve each YouTube title via MusicBrainz (with rate-limited HTTP and a
    persistent cache) and write a sldl ``.list`` file. Returns the file path,
    the count resolved via MB, and the count that fell back to cleaned YT
    titles."""
    enable_mb = env_bool("MB_LOOKUP", True)
    min_score = env_int("MB_MIN_SCORE", MB_DEFAULT_MIN_SCORE)
    cache = _load_mb_cache() if enable_mb else {}

    out = DATA_DIR / "youtube_resolved.list"
    lines: list[str] = []
    mb_hits = 0
    fallback = 0

    progress = Progress(
        SpinnerColumn(style="magenta"),
        TextColumn("[bold cyan]resolving titles via MusicBrainz"),
        BarColumn(bar_width=None),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
        transient=True,
    )
    task_id = progress.add_task("resolving", total=len(titles))
    with progress:
        for raw in titles:
            query, source = _resolve_title(raw, cache, min_score, enable_mb)
            if source == "musicbrainz":
                mb_hits += 1
            else:
                fallback += 1
            escaped = query.replace('"', '\\"')
            lines.append(f'"{escaped}"')
            progress.update(task_id, advance=1)

    if enable_mb:
        _save_mb_cache(cache)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out, mb_hits, fallback


def _txt_to_list_file(src: Path) -> Path:
    """Convert a plain `.txt` (one search query per line) into a sldl `.list` file
    by quoting each non-empty, non-comment line. The result is stored in /data so
    it survives across runs and is visible to the user."""
    out = DATA_DIR / f"{src.stem}.list"
    lines: list[str] = []
    for raw in src.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            lines.append(raw)
            continue
        # sldl strips a leading "a:" before quote-parsing, so preserve it outside the quotes.
        prefix = ""
        if stripped.startswith("a:"):
            prefix = "a:"
            stripped = stripped[2:].lstrip()
        # Avoid double-quoting if user already wrote a properly quoted entry.
        if stripped.startswith('"'):
            lines.append(f"{prefix}{stripped}")
        else:
            escaped = stripped.replace('"', '\\"')
            lines.append(f'{prefix}"{escaped}"')
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out


def parse_cli_playlist(argv: list[str]) -> str | None:
    if not argv:
        return None
    first = argv[0].strip()
    if first in {"-h", "--help"}:
        console.print("Usage: gargantua [-m|--minimal] <playlist-url>")
        console.print("Alternatively set PLAYLIST_URL or drop a .txt/.csv in /data.")
        sys.exit(0)
    if first in {"--playlist-url", "--url"}:
        if len(argv) < 2 or not argv[1].strip():
            console.print("[red]error:[/] --playlist-url expects a value.")
            sys.exit(2)
        if len(argv) > 2:
            extra = " ".join(argv[2:])
            console.print(f"[yellow]warning:[/] extra arguments ignored: {extra}")
        return _clean_url(argv[1].strip())
    if first.startswith("-"):
        console.print(f"[red]error:[/] unknown option {first!r}.")
        sys.exit(2)
    if len(argv) > 1:
        extra = " ".join(argv[1:])
        console.print(f"[yellow]warning:[/] extra arguments ignored: {extra}")
    return _clean_url(first)


def parse_cli(argv: list[str]) -> tuple[str | None, bool]:
    """Strips ``--minimal`` / ``-m`` flags from argv and delegates the rest to
    ``parse_cli_playlist``. Returns ``(url_or_None, minimal_flag)``."""
    minimal = False
    rest: list[str] = []
    for token in argv:
        if token in {"-m", "--minimal"}:
            minimal = True
        else:
            rest.append(token)
    return parse_cli_playlist(rest), minimal


def _resolve_youtube_url(url: str) -> tuple[str, str, str | None, bool]:
    """For YouTube URLs, fetch titles via yt-dlp and resolve each through
    MusicBrainz (with cleaned-title fallback) so sldl gets canonical
    ``Artist - Title`` queries instead of noisy YouTube strings.

    Single videos become sldl ``string`` inputs; playlists become a generated
    ``.list`` file in /data. Non-YouTube URLs are returned untouched. The 4th
    tuple element is True iff the original input came from YouTube."""
    video_id = _youtube_video_id(url)
    if video_id is not None:
        console.print(
            f"[cyan]Single-video YouTube URL detected[/] (id={video_id}); "
            "fetching title with yt-dlp…"
        )
        raw_title = _yt_dlp_title(video_id)
        enable_mb = env_bool("MB_LOOKUP", True)
        min_score = env_int("MB_MIN_SCORE", MB_DEFAULT_MIN_SCORE)
        cache = _load_mb_cache() if enable_mb else {}
        query, source = _resolve_title(raw_title, cache, min_score, enable_mb)
        if enable_mb and source == "musicbrainz":
            _save_mb_cache(cache)
        tag = "MusicBrainz" if source == "musicbrainz" else "cleaned title"
        console.print(f"[green]→ search query[/] ([dim]{tag}[/]): {query}")
        label = f"YouTube video ({raw_title})"
        return query, label, "string", True

    if _is_youtube_playlist(url):
        console.print(
            f"[cyan]YouTube playlist detected[/] ({url}); fetching titles with yt-dlp…"
        )
        titles = _yt_dlp_playlist_titles(url)
        console.print(
            f"[green]→ {len(titles)} videos[/]; resolving via MusicBrainz "
            f"(rate-limited at 1 req/s — first run takes ~{len(titles)}s)…"
        )
        list_file, mb_hits, fallback = _build_resolved_list(titles)
        console.print(
            f"[green]→ resolved:[/] {mb_hits} via MusicBrainz, "
            f"{fallback} via cleaned YouTube title fallback "
            f"([dim]{list_file}[/])"
        )
        label = (
            f"YouTube playlist ({url}, {len(titles)} tracks, "
            f"{mb_hits} MB / {fallback} fallback)"
        )
        return str(list_file), label, "list", True

    is_yt = "youtube.com" in url.lower() or "youtu.be" in url.lower()
    return url, _label_for_url(url), None, is_yt


def resolve_input(cli_url: str | None) -> tuple[str, str, str | None, bool]:
    """Returns (input_arg, human_label, input_type_override, youtube_origin)."""
    if cli_url:
        return _resolve_youtube_url(cli_url)
    url = (os.environ.get("PLAYLIST_URL") or os.environ.get("SPOTIFY_LINK") or "").strip()
    if url:
        url = _clean_url(url)
        return _resolve_youtube_url(url)

    txt = _find_in_data("TXT_FILENAME", ".txt", "playlist.txt")
    if txt is not None:
        list_file = _txt_to_list_file(txt)
        return str(list_file), f"text list ({txt.name}, {sum(1 for l in list_file.read_text().splitlines() if l.strip() and not l.lstrip().startswith('#'))} entries)", "list", False

    csv_path = _find_in_data("CSV_FILENAME", ".csv", "playlist.csv")
    if csv_path is not None:
        return str(csv_path), f"CSV file ({csv_path.name})", None, False

    console.print(
        "[red]error:[/] no input found. Set [bold]PLAYLIST_URL[/] (Spotify / YouTube / "
        "Bandcamp / MusicBrainz), or drop a [bold].txt[/] (one query per line) or "
        "[bold].csv[/] file in /data."
    )
    sys.exit(1)


def resolve_credentials() -> tuple[str, str, bool]:
    user = os.environ.get("SLSK_USERNAME") or os.environ.get("USERNAME") or ""
    pwd = os.environ.get("SLSK_PASSWORD") or os.environ.get("PASSWORD") or ""
    user = user.strip()
    pwd = pwd.strip()
    if user and pwd:
        return user, pwd, False
    # On Soulseek, logging in with an unknown username creates the account
    # automatically, so we can safely generate a fresh identity.
    if not user:
        user = random_username()
    if not pwd:
        pwd = random_password()
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with CREDENTIALS_FILE.open("w") as f:
            f.write(f"username={user}\npassword={pwd}\n")
        os.chmod(CREDENTIALS_FILE, 0o600)
    except OSError:
        pass
    return user, pwd, True


def load_config(cli_url: str | None, minimal_cli: bool = False) -> Config:
    user, pwd, generated = resolve_credentials()
    src, label, input_type, youtube_origin = resolve_input(cli_url)
    return Config(
        username=user,
        password=pwd,
        generated_credentials=generated,
        input_source=src,
        input_label=label,
        input_type=input_type,
        youtube_origin=youtube_origin,
        pref_format=os.environ.get("PREF_FORMAT") or os.environ.get("FORMAT") or "flac",
        name_format=os.environ.get("NAME_FORMAT") or "{artist( - )title|filename}",
        concurrent_downloads=env_int("CONCURRENT_DOWNLOADS", 4),
        strict_conditions=env_bool("STRICT_CONDITIONS", False),
        fast_search=env_bool("FAST_SEARCH", True),
        write_playlist=env_bool("WRITE_PLAYLIST", True),
        spotify_id=(os.environ.get("SPOTIFY_ID") or "").strip(),
        spotify_secret=(os.environ.get("SPOTIFY_SECRET") or "").strip(),
        extra_args=shlex.split(os.environ.get("SLDL_EXTRA_ARGS", "")),
        minimal=minimal_cli or env_bool("MINIMAL", False),
    )


# ---------- Rich rendering ---------------------------------------------------

def render_banner() -> Panel:
    text = Text(ASCII_BANNER, style="bold magenta")
    subtitle = Text(
        "Soulseek bulk music downloader · powered by sldl",
        style="italic cyan",
    )
    return Panel(
        Align.center(Group(text, Align.center(subtitle))),
        border_style="magenta",
        padding=(0, 2),
    )


def render_config(cfg: Config) -> Panel:
    table = Table.grid(padding=(0, 2), expand=False)
    table.add_column(style="bold cyan", justify="right")
    table.add_column(style="white")

    if cfg.generated_credentials:
        user_cell = Text.assemble(
            (cfg.username, "bold yellow"),
            ("  (auto-generated)", "dim italic"),
        )
    else:
        user_cell = Text(cfg.username, style="bold yellow")

    table.add_row("Soulseek user", user_cell)
    table.add_row("Source", Text(cfg.input_label, style="green"))
    table.add_row("Output", Text(str(DOWNLOADS_DIR), style="green"))
    table.add_row("Preferred format", Text(cfg.pref_format, style="bright_white"))
    table.add_row("Naming", Text(cfg.name_format, style="bright_white"))
    table.add_row("Concurrency", Text(str(cfg.concurrent_downloads), style="bright_white"))
    table.add_row("Strict conditions", Text("yes" if cfg.strict_conditions else "no", style="bright_white"))
    table.add_row("Fast search", Text("yes" if cfg.fast_search else "no", style="bright_white"))
    table.add_row("Log file", Text(str(LOG_FILE), style="dim"))

    return Panel(
        table,
        title="[bold]configuration[/]",
        border_style="cyan",
        padding=(1, 2),
    )


# ---------- sldl runner with live UI -----------------------------------------

STATUS_STYLES = {
    "downloading": ("bold blue",  "⇣"),
    "succeeded":   ("bold green", "✓"),
    "failed":      ("bold red",   "✗"),
    "skipped":     ("dim",        "·"),
    "searching":   ("yellow",     "?"),
}

# Patterns derived from sldl 2.6.0's logging (DownloadWrapper.UpdateText emits
# "{state}:" padded to 14 chars + display text; Searcher emits Searching:/
# Not found:/All downloads failed:/Out of download retries:; Printing.PrintTracksTbd
# emits "Downloading N tracks:").
RX_TRACK_TOTAL = re.compile(r"^\s*Downloading\s+(\d+)\s+tracks?\b", re.IGNORECASE)
RX_COMPLETED   = re.compile(r"^\s*Completed:\s+(\d+)\s+succeeded,\s+(\d+)\s+failed", re.IGNORECASE)
RX_INIT        = re.compile(r"^\s*(?:Initialize|Initializing|Queued(?:\s*\([LR]\))?|Requested|Connecting):\s+(.+)$")
RX_PROGRESS    = re.compile(r"^\s*(?:InProgress|Downloading):\s+(.+)$")
RX_STALLED     = re.compile(r"^\s*Stalled:\s+(.+)$")
RX_SUCCESS     = re.compile(r"^\s*Succe[ed]+ed[^:]*:\s+(.+)$")
RX_FAIL        = re.compile(
    r"^\s*(?:Errored|Cancelled|TimedOut|Rejected|Aborted|Not found|"
    r"All downloads failed|Out of download retries|Failed):\s+(.+)$"
)
RX_SEARCHING   = re.compile(r"^\s*Searching:\s+(.+)$")
RX_SKIP        = re.compile(
    r"^\s*\S+\s+download\s+'.+?'.+?(?:already exists|not found during a prior).*?skipping",
    re.IGNORECASE,
)
RX_LOGIN_FAIL  = re.compile(r"\blogin\b.*\b(failed|invalid|incorrect)", re.IGNORECASE)


@dataclass
class RunState:
    total: int | None = None
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    in_progress: dict[str, str] = field(default_factory=dict)
    recent: deque = field(default_factory=lambda: deque(maxlen=12))
    last_log: deque = field(default_factory=lambda: deque(maxlen=4))
    failed_tracks: list[str] = field(default_factory=list)

    @property
    def done(self) -> int:
        return self.succeeded + self.failed + self.skipped


def make_layout(state: RunState, progress: Progress) -> Group:
    active = [(t, i) for t, i in state.in_progress.items() if i == "downloading"]
    queued = [(t, i) for t, i in state.in_progress.items() if i != "downloading"]

    counters = Table.grid(padding=(0, 3))
    for _ in range(4):
        counters.add_column(justify="center")
    counters.add_row(
        Text.assemble(("succeeded\n", "dim"), (str(state.succeeded), "bold green")),
        Text.assemble(("failed\n", "dim"), (str(state.failed), "bold red")),
        Text.assemble(("skipped\n", "dim"), (str(state.skipped), "dim")),
        Text.assemble(("in flight\n", "dim"), (str(len(active)), "bold blue")),
    )

    if active or queued:
        live_tbl = Table(
            show_header=True,
            header_style="bold cyan",
            border_style="grey39",
            expand=True,
            pad_edge=False,
        )
        live_tbl.add_column("track", overflow="fold", ratio=2)
        live_tbl.add_column("status", style="dim", ratio=1, no_wrap=True)
        rows = active + queued
        for track, info in rows[:6]:
            style, _ = STATUS_STYLES.get(
                "downloading" if info == "downloading" else "searching", ("white", "•"),
            )
            label = info or "downloading"
            live_tbl.add_row(Text(track, style=style), Text(label, style=style))
        in_flight_panel = Panel(live_tbl, title="[bold blue]downloading[/]", border_style="blue")
    else:
        in_flight_panel = Panel(
            Align.center(Text("waiting for sldl…", style="dim italic")),
            title="[bold blue]downloading[/]",
            border_style="blue",
        )

    if state.recent:
        recent_tbl = Table(
            show_header=False,
            border_style="grey39",
            expand=True,
            pad_edge=False,
        )
        recent_tbl.add_column(width=2)
        recent_tbl.add_column(overflow="fold")
        for status, track in list(state.recent)[-8:]:
            style, glyph = STATUS_STYLES.get(status, ("white", "•"))
            recent_tbl.add_row(Text(glyph, style=style), Text(track, style=style))
        recent_panel = Panel(recent_tbl, title="[bold]recent activity[/]", border_style="grey50")
    else:
        recent_panel = Panel(
            Align.center(Text("no completed tracks yet", style="dim italic")),
            title="[bold]recent activity[/]",
            border_style="grey50",
        )

    return Group(
        Panel(counters, border_style="magenta", padding=(0, 1)),
        progress,
        in_flight_panel,
        recent_panel,
    )


def _short(track: str, limit: int = 80) -> str:
    track = track.strip()
    return track if len(track) <= limit else track[: limit - 1] + "…"


def parse_line(line: str, state: RunState, progress: Progress, task_id) -> None:
    stripped = line.strip()
    if not stripped:
        return

    state.last_log.append(stripped)

    if state.total is None:
        if (m := RX_TRACK_TOTAL.match(stripped)):
            try:
                value = int(m.group(1))
                if value > 0:
                    state.total = value
                    progress.update(task_id, total=value)
            except ValueError:
                pass
            return

    if RX_LOGIN_FAIL.search(stripped):
        state.recent.append(("failed", f"login: {_short(stripped)}"))
        return

    if (m := RX_SUCCESS.match(stripped)):
        track = _short(m.group(1))
        state.in_progress.pop(track, None)
        state.succeeded += 1
        state.recent.append(("succeeded", track))
        progress.update(task_id, completed=state.done)
        return

    if (m := RX_FAIL.match(stripped)):
        track = _short(m.group(1))
        state.in_progress.pop(track, None)
        state.failed += 1
        state.recent.append(("failed", track))
        state.failed_tracks.append(m.group(1).strip())
        progress.update(task_id, completed=state.done)
        return

    if RX_SKIP.search(stripped):
        state.skipped += 1
        state.recent.append(("skipped", _short(stripped)))
        progress.update(task_id, completed=state.done)
        return

    if (m := RX_PROGRESS.match(stripped)):
        track = _short(m.group(1))
        state.in_progress[track] = "downloading"
        return

    if (m := RX_INIT.match(stripped)):
        track = _short(m.group(1))
        state.in_progress.setdefault(track, "queued")
        return

    if (m := RX_STALLED.match(stripped)):
        track = _short(m.group(1))
        state.in_progress[track] = "stalled"
        return

    if (m := RX_SEARCHING.match(stripped)):
        track = _short(m.group(1))
        state.in_progress.setdefault(track, "searching")
        return


def build_command(cfg: Config) -> list[str]:
    cmd = [
        "sldl",
        cfg.input_source,
        "--user", cfg.username,
        "--pass", cfg.password,
        "--path", str(DOWNLOADS_DIR),
        "--pref-format", cfg.pref_format,
        "--name-format", cfg.name_format,
        "--concurrent-downloads", str(cfg.concurrent_downloads),
        "--log-file", str(LOG_FILE),
        "--index-path", str(INDEX_PATH),
        "--no-progress",
    ]
    if cfg.input_type:
        cmd.extend(["--input-type", cfg.input_type])
    if cfg.strict_conditions:
        cmd.append("--strict-conditions")
    if cfg.fast_search:
        cmd.append("--fast-search")
    if cfg.write_playlist:
        cmd.append("--write-playlist")
    if cfg.spotify_id:
        cmd.extend(["--spotify-id", cfg.spotify_id])
    if cfg.spotify_secret:
        cmd.extend(["--spotify-secret", cfg.spotify_secret])
    cmd.extend(cfg.extra_args)
    return cmd


def parse_line_minimal(line: str, state: RunState, progress: Progress, task_id) -> None:
    """Stripped-down event parser for minimal mode: only updates counters,
    progress, and prints one line per terminal event (succeeded/failed/skipped).
    No in-flight tracking, no recent buffer, no panels."""
    stripped = line.strip()
    if not stripped:
        return
    state.last_log.append(stripped)

    if state.total is None and (m := RX_TRACK_TOTAL.match(stripped)):
        try:
            value = int(m.group(1))
            if value > 0:
                state.total = value
                progress.update(task_id, total=value)
        except ValueError:
            pass
        return

    if RX_LOGIN_FAIL.search(stripped):
        return

    if (m := RX_SUCCESS.match(stripped)):
        track = m.group(1).strip()
        state.succeeded += 1
        console.print(f"[green]✓[/] {track}")
        progress.update(task_id, completed=state.done)
        return

    if (m := RX_FAIL.match(stripped)):
        track = m.group(1).strip()
        state.failed += 1
        state.failed_tracks.append(track)
        console.print(f"[red]✗[/] {track}")
        progress.update(task_id, completed=state.done)
        return

    if RX_SKIP.search(stripped):
        state.skipped += 1
        console.print(f"[dim]·[/] {_short(stripped, 100)}")
        progress.update(task_id, completed=state.done)
        return


def stream_subprocess(
    cmd: Iterable[str], minimal: bool = False, initial_total: int | None = None
) -> tuple[int, RunState]:
    state = RunState()
    if initial_total is not None and initial_total > 0:
        state.total = initial_total

    proc = subprocess.Popen(
        list(cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    def _forward_signal(signum, _frame):
        if proc.poll() is None:
            proc.send_signal(signum)

    signal.signal(signal.SIGTERM, _forward_signal)
    signal.signal(signal.SIGINT, _forward_signal)

    if minimal:
        progress = Progress(
            TextColumn("[bold cyan]downloading"),
            BarColumn(bar_width=None),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=console,
            expand=True,
        )
        task_id = progress.add_task("downloading", total=state.total)
        with progress:
            assert proc.stdout is not None
            for line in proc.stdout:
                parse_line_minimal(line, state, progress, task_id)
    else:
        progress = Progress(
            SpinnerColumn(style="magenta"),
            TextColumn("[bold]{task.description}", justify="left"),
            BarColumn(bar_width=None, complete_style="green", finished_style="bold green"),
            MofNCompleteColumn(),
            TextColumn("[dim]elapsed"),
            TimeElapsedColumn(),
            expand=True,
        )
        task_id = progress.add_task("downloading", total=state.total)
        with Live(make_layout(state, progress), console=console, refresh_per_second=8, screen=False) as live:
            assert proc.stdout is not None
            for line in proc.stdout:
                parse_line(line, state, progress, task_id)
                live.update(make_layout(state, progress))

    return proc.wait(), state


def _title_only(track: str) -> str | None:
    """Drop the leading ``Artist - `` from a sldl-formatted track string. sldl
    splits on the first `` - `` to get artist/title, so we mirror that."""
    if " - " not in track:
        return None
    title = track.split(" - ", 1)[1].strip()
    return title or None


def _build_retry_list(failed_tracks: list[str]) -> tuple[Path, list[str]] | None:
    """Build a sldl ``--input-type=list`` file from failed tracks, keeping only
    the title (no ``Artist -`` prefix). Returns the file path and the list of
    queries, or None if nothing is retryable."""
    titles: list[str] = []
    seen: set[str] = set()
    for track in failed_tracks:
        title = _title_only(track)
        if not title or title in seen:
            continue
        seen.add(title)
        titles.append(title)
    if not titles:
        return None
    out = DATA_DIR / "youtube_title_retry.list"
    lines = []
    for t in titles:
        escaped = t.replace('"', '\\"')
        lines.append(f'"{escaped}"')
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out, titles


def build_retry_command(cfg: Config, list_file: Path) -> list[str]:
    cmd = [
        "sldl",
        str(list_file),
        "--input-type", "list",
        "--user", cfg.username,
        "--pass", cfg.password,
        "--path", str(DOWNLOADS_DIR),
        "--pref-format", cfg.pref_format,
        "--name-format", cfg.name_format,
        "--concurrent-downloads", str(cfg.concurrent_downloads),
        "--log-file", str(LOG_FILE),
        "--index-path", str(INDEX_PATH),
        "--no-progress",
    ]
    if cfg.strict_conditions:
        cmd.append("--strict-conditions")
    if cfg.fast_search:
        cmd.append("--fast-search")
    if cfg.write_playlist:
        cmd.append("--write-playlist")
    cmd.extend(cfg.extra_args)
    return cmd


def render_summary(state: RunState, exit_code: int) -> Panel:
    headline = "completed" if exit_code == 0 else f"sldl exited with code {exit_code}"
    style = "bold green" if exit_code == 0 else "bold red"

    body = Table.grid(padding=(0, 3))
    body.add_column(justify="right", style="bold")
    body.add_column()
    body.add_row("succeeded", Text(str(state.succeeded), style="green"))
    body.add_row("failed", Text(str(state.failed), style="red"))
    body.add_row("skipped", Text(str(state.skipped), style="yellow"))
    if state.total:
        body.add_row("total", Text(str(state.total)))

    if state.last_log and exit_code != 0:
        tail = Text("\n".join(state.last_log), style="dim")
        body.add_row("last log", tail)

    return Panel(body, title=Text(headline, style=style), border_style=style.split()[-1])


# ---------- Main -------------------------------------------------------------

def _print_minimal_summary(state: RunState, exit_code: int) -> None:
    label = "done" if exit_code == 0 else f"sldl exited with code {exit_code}"
    style = "bold green" if exit_code == 0 else "bold red"
    console.print(
        f"[{style}]{label}.[/] "
        f"[green]{state.succeeded} ok[/], "
        f"[red]{state.failed} failed[/], "
        f"[dim]{state.skipped} skipped[/]"
    )


def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

    cli_url, minimal_cli = parse_cli(sys.argv[1:])
    cfg = load_config(cli_url, minimal_cli)

    if cfg.minimal:
        cred_note = " (auto credentials)" if cfg.generated_credentials else ""
        console.print(
            f"[bold]gargantua:[/] {cfg.input_label} → "
            f"{DOWNLOADS_DIR} ({cfg.pref_format}){cred_note}"
        )
    else:
        console.print(render_banner())
        console.print(render_config(cfg))
        if cfg.generated_credentials:
            console.print(
                Panel(
                    Text(
                        "No SLSK_USERNAME / SLSK_PASSWORD provided.\n"
                        "A throwaway Soulseek identity has been generated.\n"
                        f"Credentials saved to {CREDENTIALS_FILE} (mode 600).",
                        style="yellow",
                    ),
                    border_style="yellow",
                    title="[bold]auto-generated credentials[/]",
                )
            )
        console.rule("[bold magenta]running sldl[/]")

    cmd = build_command(cfg)
    initial_total = 1 if cfg.input_type == "string" else None
    try:
        exit_code, state = stream_subprocess(
            cmd, minimal=cfg.minimal, initial_total=initial_total
        )
    except FileNotFoundError:
        console.print("[red]error:[/] sldl binary is not on PATH inside the container.")
        return 127

    if cfg.minimal:
        _print_minimal_summary(state, exit_code)
    else:
        console.rule("[bold magenta]done[/]")
        console.print(render_summary(state, exit_code))

    if (
        env_bool("YOUTUBE_TITLE_FALLBACK", True)
        and cfg.youtube_origin
        and state.failed_tracks
    ):
        retry = _build_retry_list(state.failed_tracks)
        if retry is not None:
            list_file, titles = retry
            if cfg.minimal:
                console.print(f"[bold cyan]retry:[/] {len(titles)} track(s) by title only")
            else:
                console.rule(f"[bold magenta]retry: {len(titles)} track(s) by title only[/]")
                console.print(
                    Panel(
                        Text(
                            "Retrying failed YouTube tracks with the title only "
                            "(no \"Artist -\" prefix).\n"
                            f"Queries written to {list_file}.",
                            style="cyan",
                        ),
                        border_style="cyan",
                        title="[bold]title-only fallback[/]",
                    )
                )
            try:
                retry_exit, retry_state = stream_subprocess(
                    build_retry_command(cfg, list_file), minimal=cfg.minimal
                )
            except FileNotFoundError:
                console.print("[red]error:[/] sldl binary disappeared between runs.")
                return 127
            if cfg.minimal:
                _print_minimal_summary(retry_state, retry_exit)
            else:
                console.rule("[bold magenta]retry done[/]")
                console.print(render_summary(retry_state, retry_exit))
            state.succeeded += retry_state.succeeded
            state.failed = max(state.failed - retry_state.succeeded, 0)
            if retry_exit != 0 and exit_code == 0:
                exit_code = retry_exit

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
