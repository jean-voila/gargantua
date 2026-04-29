#!/usr/bin/env python3
"""Gargantua entrypoint: Soulseek bulk downloader on top of sldl with a rich TUI."""
from __future__ import annotations

import os
import re
import secrets
import shlex
import signal
import string
import subprocess
import sys
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

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


def resolve_input() -> tuple[str, str, str | None]:
    """Returns (input_arg, human_label, input_type_override)."""
    url = (os.environ.get("PLAYLIST_URL") or os.environ.get("SPOTIFY_LINK") or "").strip()
    if url:
        return url, _label_for_url(url), None

    txt = _find_in_data("TXT_FILENAME", ".txt", "playlist.txt")
    if txt is not None:
        list_file = _txt_to_list_file(txt)
        return str(list_file), f"text list ({txt.name}, {sum(1 for l in list_file.read_text().splitlines() if l.strip() and not l.lstrip().startswith('#'))} entries)", "list"

    csv_path = _find_in_data("CSV_FILENAME", ".csv", "playlist.csv")
    if csv_path is not None:
        return str(csv_path), f"CSV file ({csv_path.name})", None

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


def load_config() -> Config:
    user, pwd, generated = resolve_credentials()
    src, label, input_type = resolve_input()
    return Config(
        username=user,
        password=pwd,
        generated_credentials=generated,
        input_source=src,
        input_label=label,
        input_type=input_type,
        pref_format=os.environ.get("PREF_FORMAT") or os.environ.get("FORMAT") or "flac",
        name_format=os.environ.get("NAME_FORMAT") or "{artist( - )title|filename}",
        concurrent_downloads=env_int("CONCURRENT_DOWNLOADS", 4),
        strict_conditions=env_bool("STRICT_CONDITIONS", False),
        fast_search=env_bool("FAST_SEARCH", True),
        write_playlist=env_bool("WRITE_PLAYLIST", True),
        spotify_id=(os.environ.get("SPOTIFY_ID") or "").strip(),
        spotify_secret=(os.environ.get("SPOTIFY_SECRET") or "").strip(),
        extra_args=shlex.split(os.environ.get("SLDL_EXTRA_ARGS", "")),
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


def stream_subprocess(cmd: Iterable[str]) -> tuple[int, RunState]:
    state = RunState()
    progress = Progress(
        SpinnerColumn(style="magenta"),
        TextColumn("[bold]{task.description}", justify="left"),
        BarColumn(bar_width=None, complete_style="green", finished_style="bold green"),
        MofNCompleteColumn(),
        TextColumn("[dim]elapsed"),
        TimeElapsedColumn(),
        expand=True,
    )
    task_id = progress.add_task("downloading", total=None)

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

    with Live(make_layout(state, progress), console=console, refresh_per_second=8, screen=False) as live:
        assert proc.stdout is not None
        for line in proc.stdout:
            parse_line(line, state, progress, task_id)
            live.update(make_layout(state, progress))

    return proc.wait(), state


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

def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

    cfg = load_config()

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
    try:
        exit_code, state = stream_subprocess(cmd)
    except FileNotFoundError:
        console.print("[red]error:[/] sldl binary is not on PATH inside the container.")
        return 127

    console.rule("[bold magenta]done[/]")
    console.print(render_summary(state, exit_code))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
