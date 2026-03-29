#!/usr/bin/env python3
"""
tdupes - Find and manage duplicate files.

Scans files and directories for exact duplicates (via fdupes) and, optionally,
near-duplicates sharing the same basename (via plocate/locate).  Results are
written to a TSV that can be reviewed and edited before any files are touched.
Confirmed deletions are sent to the system trash via gio trash, so nothing is
irrecoverably lost until the bin is emptied.

Features
--------
* Exact-duplicate detection via fdupes (byte-identical, any mix of files/dirs)
* Near-duplicate detection via -L (same basename, scored by similarity)
* Preferred-directory protection: files inside preferred dirs are never proposed to be deleted by default
* Exclusion patterns: skip files matching shell glob patterns
* Interactive review: TSV opened with xdg-open; edit Action column, then confirm
* Batch mode: no prompts, immediate execution (suitable for scripting/cron)
* Safe deletion: gio trash keeps files recoverable
"""
from __future__ import annotations

import argparse
import difflib
import fnmatch
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import NamedTuple

from . import __version__

try:
    import yaml
    _HAS_YAML = True
except ImportError:
    _HAS_YAML = False


# ── Verbosity ─────────────────────────────────────────────────────────────────

QUIET = 0
NORMAL = 1
VERBOSE = 2

_verbosity: int = NORMAL


def vprint(msg: str = "", level: int = NORMAL, **kw) -> None:
    if _verbosity >= level:
        print(msg, **kw)


def eprint(msg: str) -> None:
    print(msg, file=sys.stderr)


# ── Dependency check ──────────────────────────────────────────────────────────

_REQUIRED_BINS = {
    "fdupes":    "fdupes          →  sudo apt install fdupes",
    "gio":       "gio (GVfs)      →  sudo apt install gvfs-bin",
    "xdg-open":  "xdg-open        →  sudo apt install xdg-utils",
}
_LOCATE_LABEL = "plocate/locate  →  sudo apt install plocate"


def _locate_binary() -> str | None:
    """Return the best available locate binary: plocate preferred over locate."""
    for binary in ("plocate", "locate"):
        if shutil.which(binary):
            return binary
    return None


def check_dependencies(need_locate: bool) -> list[str]:
    missing: list[str] = []
    if not _HAS_YAML:
        missing.append("PyYAML          →  pip install pyyaml")
    for binary, label in _REQUIRED_BINS.items():
        if not shutil.which(binary):
            missing.append(label)
    if need_locate and _locate_binary() is None:
        missing.append(_LOCATE_LABEL)
    return missing


# ── Config ────────────────────────────────────────────────────────────────────

DEFAULT_CONFIG: dict = {
    "preferred_directories": [],
    "verbosity": NORMAL,
    "tsv_output": None,
    "exclusion_patterns": [],
    "batch_mode": False,
}

_CONFIG_HEADER = """\
# tdupes configuration
#
# preferred_directories  - list of directory paths; files inside are never
#                          recommended for deletion.
# verbosity              - 0=quiet, 1=normal, 2=verbose
# tsv_output             - absolute path for TSV output file, or null (temp)
# exclusion_patterns     - shell glob patterns matched against full file paths;
#                          matching files are excluded from results.
# batch_mode             - true: no prompts; actions execute immediately;
#                          TSV is a log of what was done.
#
"""


def get_config_path() -> Path:
    xdg = os.environ.get("XDG_CONFIG_HOME", "")
    base = Path(xdg) if xdg else Path.home() / ".config"
    return base / "tdupes.yml"


def load_config(override: Path | None = None) -> dict:
    path = override or get_config_path()
    cfg = dict(DEFAULT_CONFIG)

    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as fh:
            fh.write(_CONFIG_HEADER)
            yaml.dump(DEFAULT_CONFIG, fh, default_flow_style=False)
        print(f"[tdupes] Created default config at {path}")
        print("[tdupes] Edit preferred_directories and other options as needed.\n")
        return cfg

    with path.open() as fh:
        data = yaml.safe_load(fh) or {}

    for key in DEFAULT_CONFIG:
        if key in data:
            cfg[key] = data[key]
    return cfg


# ── locate integration ────────────────────────────────────────────────────────

def locate_by_basenames(cli_files: set[Path]) -> dict[Path, list[Path]]:
    """
    Return a mapping {cli_file: [located_paths_with_same_basename, ...]} for all
    cli_files in a single locate invocation (one subprocess call regardless of N).

    locate -b accepts multiple patterns; each pattern '\name' anchors to the
    basename.  Results are filtered back to each originating cli_file by name.
    Multiple cli files can share the same basename — all matches are returned for
    each of them (excluding the cli file itself).
    """
    if not cli_files:
        return {}

    # Build one pattern per unique basename.
    by_name: dict[str, list[Path]] = {}
    for cf in cli_files:
        by_name.setdefault(cf.name, []).append(cf)

    binary = _locate_binary() or "locate"
    patterns = [f"\\{name}" for name in by_name]
    try:
        result = subprocess.run(
            [binary, "-b"] + patterns,
            capture_output=True,
            text=True,
        )
    except Exception as exc:
        eprint(f"[{binary}] warning: {exc}")
        return {cf: [] for cf in cli_files}

    # Map results back: a result line belongs to every cli_file with that basename.
    found_by_name: dict[str, list[Path]] = {name: [] for name in by_name}
    for line in result.stdout.splitlines():
        p = Path(line.strip())
        name = p.name
        if name in found_by_name and p.is_file():
            found_by_name[name].append(p)

    result_map: dict[Path, list[Path]] = {}
    for cf in cli_files:
        siblings = [p for p in found_by_name[cf.name] if p != cf]
        result_map[cf] = siblings
    return result_map


# ── fdupes integration ────────────────────────────────────────────────────────

def _fdupes_dirs(dirs: list[Path], extra_flags: list[str] = []) -> list[list[Path]]:
    """Invoke fdupes on *dirs* and return groups of exact duplicates."""
    if not dirs:
        return []
    cmd = ["fdupes", "--recurse", "--quiet"] + extra_flags + [str(p) for p in dirs]
    vprint(f"  $ {' '.join(cmd)}", VERBOSE)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True)
    except FileNotFoundError:
        eprint("Error: fdupes not found.")
        sys.exit(1)

    groups: list[list[Path]] = []
    current: list[Path] = []
    for line in proc.stdout.splitlines():
        stripped = line.strip()
        if not stripped:
            if current:
                groups.append(current)
                current = []
        else:
            p = Path(stripped)
            if p.exists():
                current.append(p)
    if current:
        groups.append(current)
    return groups


def run_fdupes(paths: list[Path]) -> list[list[Path]]:
    """Run fdupes and return groups of exact duplicates.

    fdupes only accepts directory arguments.  Any individual files in *paths*
    are staged via symlinks inside a temporary directory so fdupes can compare
    them.  One sub-folder is created per unique basename to avoid collisions
    when multiple files share the same name.  Staged paths in the output are
    translated back to the original paths before returning.

    Symlinks (rather than hard links) are used to avoid a fdupes quirk: fdupes
    normally treats same-inode pairs as the same file and skips them, which
    would silently drop hard-linked duplicates.  Symlinks have their own inodes,
    and fdupes follows them correctly when passed --symlinks.
    """
    dirs  = [p for p in paths if p.is_dir()]
    files = [p for p in paths if p.is_file()]

    if not files:
        return _fdupes_dirs(dirs)

    with tempfile.TemporaryDirectory(prefix="tdupes_stage_") as tmp_str:
        stage = Path(tmp_str)
        link_map: dict[Path, Path] = {}  # staged path → original path

        # Group files by basename; each basename gets its own set of subdirs
        # (0/, 1/, …) so files with the same name don't overwrite each other.
        by_name: dict[str, list[Path]] = {}
        for f in files:
            by_name.setdefault(f.name, []).append(f)

        for name, file_list in by_name.items():
            for i, orig in enumerate(file_list):
                subdir = stage / str(i)
                subdir.mkdir(exist_ok=True)
                dest = subdir / name
                os.symlink(orig, dest)
                link_map[dest] = orig

        groups = _fdupes_dirs(dirs + [stage], extra_flags=["--symlinks"])

        # Translate staged paths back to their originals
        def xlat(p: Path) -> Path:
            return link_map.get(p, p)

        return [[xlat(p) for p in grp] for grp in groups]


# ── Similarity ────────────────────────────────────────────────────────────────

def _is_binary(path: Path) -> bool:
    try:
        with path.open("rb") as fh:
            return b"\x00" in fh.read(8192)
    except OSError:
        return True


def similarity_code(ref: Path, other: Path) -> str:
    """
    "100" - byte-identical (guaranteed for all fdupes group members)
    "XXX" - binary, same size, not identical
    "NNN" - text, N% match via difflib (000–099; "100" reserved for exact)
    "!!!" - binary, different size
    """
    try:
        sz_ref = ref.stat().st_size
        sz_other = other.stat().st_size
    except OSError:
        return "!!!"

    bin_ref = _is_binary(ref)
    bin_oth = _is_binary(other)

    if bin_ref or bin_oth:
        return "XXX" if sz_ref == sz_other else "!!!"

    try:
        t1 = ref.read_text(errors="replace")
        t2 = other.read_text(errors="replace")
        pct = int(difflib.SequenceMatcher(None, t1, t2).ratio() * 100)
        return "100" if pct == 100 else str(pct).zfill(3)
    except Exception:
        return "!!!"


# ── TSV model ─────────────────────────────────────────────────────────────────

HEADER = ["Action", "Similarity", "Size_KB", "Modified", "Path", "Comment"]


class FileEntry(NamedTuple):
    action: str      # "DELETE" or "keep"
    similarity: str  # "100", "XXX", "NNN", "!!!"
    size_kb: float
    modified: str    # ISO-8601
    path: Path
    comment: str = ""  # human-readable reason for the action


def _stat_safe(path: Path) -> os.stat_result | None:
    try:
        return path.stat()
    except OSError:
        return None


def _in_preferred(path: Path, preferred: list[str]) -> bool:
    resolved = path.resolve()
    for pref in preferred:
        try:
            resolved.relative_to(Path(pref).resolve())
            return True
        except ValueError:
            pass
    return False


def _mtime(path: Path) -> float:
    st = _stat_safe(path)
    return st.st_mtime if st else 0.0


def _fsize(path: Path) -> int:
    st = _stat_safe(path)
    return st.st_size if st else 0


def _near_dupe_keep_set(paths: list[Path], preferred: list[str]) -> set[Path]:
    """
    Determine which paths to keep in a near-dupe group.
    All preferred-directory files are kept.
    Among non-preferred files, keep the largest by size AND the newest by mtime
    ONLY if no preferred file already holds that distinction (i.e. if a preferred
    file is already the overall largest, no extra non-preferred keeper is added for
    size; likewise for mtime). They may resolve to the same file.
    """
    pref = {p for p in paths if _in_preferred(p, preferred)}
    keep = set(pref)
    if paths:
        largest_overall = max(paths, key=_fsize)
        newest_overall  = max(paths, key=_mtime)
        if largest_overall not in pref:
            keep.add(largest_overall)
        if newest_overall not in pref:
            keep.add(newest_overall)
    return keep


def apply_exclusions(groups: list[list[Path]], patterns: list[str]) -> list[list[Path]]:
    if not patterns:
        return groups
    result: list[list[Path]] = []
    for group in groups:
        kept = [p for p in group if not any(fnmatch.fnmatch(str(p), pat) for pat in patterns)]
        if len(kept) > 1:
            result.append(kept)
    return result


def build_tsv_groups(
    fdupes_groups: list[list[Path]],
    cli_files: set[Path],
    preferred: list[str],
) -> list[list[FileEntry]]:
    out: list[list[FileEntry]] = []

    for raw in fdupes_groups:
        if len(raw) < 2:
            continue

        # CLI-specified files go first; the rest sort newest→oldest (oldest last).
        cli_members = [p for p in raw if p in cli_files]
        other_members = [p for p in raw if p not in cli_files]
        other_members.sort(
            key=lambda p: (st := _stat_safe(p)) and st.st_mtime or 0,
            reverse=True,
        )
        ordered = cli_members + other_members

        entries: list[FileEntry] = []
        for i, path in enumerate(ordered):
            st = _stat_safe(path)
            size_kb = (st.st_size / 1024) if st else 0.0
            modified = (
                datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds")
                if st else ""
            )
            # fdupes guarantees byte-identical matches → always "100"
            sim = "100"
            in_pref = _in_preferred(path, preferred)
            is_last = i == len(ordered) - 1
            action = "keep" if (in_pref or is_last) else "DELETE"

            reasons: list[str] = []
            if in_pref:
                reasons.append("in preferred folder")
            if is_last and not in_pref:
                reasons.append("last in group")
            comment = ", ".join(reasons)

            entries.append(FileEntry(action, sim, size_kb, modified, path, comment))

        out.append(entries)
    return out


def build_near_dupe_groups(
    cli_files: set[Path],
    locate_map: dict[Path, list[Path]],
    fdupes_groups: list[list[Path]],
    preferred: list[str],
) -> list[list[FileEntry]]:
    """
    Build TSV groups for near-duplicates found with -L.
    For each CLI file, locate matches that are NOT already exact duplicates of it
    (per fdupes) are collected into a group and given real similarity codes.
    The keep/DELETE action uses the near-dupe rule: keep preferred-dir files,
    and among the rest keep the largest AND the newest (possibly the same file).
    """
    # Map each path → frozenset of its exact-dupe companions (including itself)
    exact_group_of: dict[Path, frozenset[Path]] = {}
    for group in fdupes_groups:
        fs = frozenset(group)
        for p in group:
            exact_group_of[p] = fs

    out: list[list[FileEntry]] = []

    for cli_file in sorted(cli_files):
        matches = locate_map.get(cli_file, [])
        if not matches:
            continue

        cli_exact = exact_group_of.get(cli_file, frozenset())
        near = [m for m in matches if m not in cli_exact and m.exists()]
        if not near:
            continue

        # CLI file first; remaining sorted newest → oldest
        near.sort(key=_mtime, reverse=True)
        ordered = [cli_file] + near

        # Compute keep set and identify which file earns each keep tag.
        # Largest/newest non-preferred keepers are only added when no preferred
        # file already holds that distinction across the whole group.
        pref_set = {p for p in ordered if _in_preferred(p, preferred)}
        keep_set = set(pref_set)

        largest_overall = max(ordered, key=_fsize)
        newest_overall  = max(ordered, key=_mtime)

        largest_np = largest_overall if largest_overall not in pref_set else None
        newest_np  = newest_overall  if newest_overall  not in pref_set else None

        if largest_np:
            keep_set.add(largest_np)
        if newest_np:
            keep_set.add(newest_np)

        entries: list[FileEntry] = []
        for i, path in enumerate(ordered):
            st = _stat_safe(path)
            size_kb = (st.st_size / 1024) if st else 0.0
            modified = (
                datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds")
                if st else ""
            )
            sim = "100" if i == 0 else similarity_code(cli_file, path)
            action = "keep" if path in keep_set else "DELETE"

            reasons: list[str] = []
            if path in pref_set:
                reasons.append("in preferred folder")
            if path is largest_np:
                reasons.append("largest in basename group")
            if path is newest_np:
                reasons.append("newest in basename group")
            comment = ", ".join(reasons)

            entries.append(FileEntry(action, sim, size_kb, modified, path, comment))

        out.append(entries)

    return out


def _entry_cells(e: FileEntry) -> list[str]:
    return [e.action, e.similarity, f"{e.size_kb:.1f}", e.modified, str(e.path), e.comment]


def _write_groups(fh, groups: list[list[FileEntry]], first_group: bool = True) -> None:
    for grp in groups:
        if not first_group:
            fh.write("\n")
        first_group = False
        for e in grp:
            fh.write("\t".join(_entry_cells(e)) + "\n")


def write_tsv(
    groups: list[list[FileEntry]],
    dest: Path,
    near_dupe_groups: list[list[FileEntry]] | None = None,
) -> None:
    with dest.open("w") as fh:
        fh.write("\t".join(HEADER) + "\n")
        _write_groups(fh, groups, first_group=True)
        if near_dupe_groups:
            fh.write("\n#\t\t\t\tNEAR-DUPLICATES — same basename, not byte-identical\n")
            _write_groups(fh, near_dupe_groups, first_group=True)


def _parse_tsv_for_display(
    path: Path,
) -> tuple[list[list[FileEntry]], list[list[FileEntry]]]:
    """
    Re-read a (possibly user-edited) TSV file and reconstruct two lists of
    FileEntry groups — (exact_groups, near_groups) — suitable for print_tsv_table.
    The split is detected by the special near-duplicates comment line.
    Blank lines delimit groups; comment/header lines are skipped.
    """
    lines = path.read_text().splitlines()
    in_near = False
    exact_groups: list[list[FileEntry]] = []
    near_groups: list[list[FileEntry]] = []
    current: list[FileEntry] = []

    def flush(target: list[list[FileEntry]]) -> None:
        if current:
            target.append(list(current))
            current.clear()

    for line in lines:
        if not line:
            if in_near:
                flush(near_groups)
            else:
                flush(exact_groups)
            continue
        if line.startswith("Action\t"):
            continue
        if line.startswith("#\t\t\t\tNEAR-DUPLICATES"):
            flush(exact_groups)
            in_near = True
            continue
        if line.startswith("#"):
            continue
        cols = line.split("\t")
        if len(cols) < 5:
            continue
        action = cols[0].strip()
        sim = cols[1].strip() if len(cols) > 1 else ""
        size_kb_s = cols[2].strip() if len(cols) > 2 else "0"
        modified = cols[3].strip() if len(cols) > 3 else ""
        path_str = cols[4].strip()
        comment = cols[5].strip() if len(cols) > 5 else ""
        try:
            size_kb = float(size_kb_s)
        except ValueError:
            size_kb = 0.0
        current.append(FileEntry(action, sim, size_kb, modified, Path(path_str), comment))

    if in_near:
        flush(near_groups)
    else:
        flush(exact_groups)

    return exact_groups, near_groups


def read_tsv(path: Path) -> list[tuple[str, str]]:
    """Return [(action, path_str), ...] from a (possibly user-edited) TSV.
    Skips the header, blank lines, and comment lines starting with '#'.
    """
    rows: list[tuple[str, str]] = []
    for line in path.read_text().splitlines():
        if not line or line.startswith("Action\t") or line.startswith("#"):
            continue
        cols = line.split("\t")
        if len(cols) >= 5:
            rows.append((cols[0].strip(), cols[4].strip()))
    return rows


# ── Display ───────────────────────────────────────────────────────────────────

def _col_widths(groups: list[list[FileEntry]]) -> list[int]:
    widths = [len(h) for h in HEADER]
    for grp in groups:
        for e in grp:
            for i, cell in enumerate(_entry_cells(e)):
                widths[i] = max(widths[i], len(cell))
    return widths


def print_tsv_table(groups: list[list[FileEntry]], title: str | None = None) -> None:
    if title:
        print(title)
    widths = _col_widths(groups)
    sep = "  "

    def fmt(cells: list[str]) -> str:
        return sep.join(c.ljust(widths[i]) for i, c in enumerate(cells))

    print(fmt(HEADER))
    print("─" * (sum(widths) + len(sep) * (len(widths) - 1)))
    for gi, grp in enumerate(groups):
        if gi > 0:
            print()
        for e in grp:
            print(fmt(_entry_cells(e)))


def _tally(actions: list[tuple[str, str]]) -> tuple[int, float, int, float]:
    n_del = n_keep = 0
    kb_del = kb_keep = 0.0
    for action, path_str in actions:
        st = _stat_safe(Path(path_str))
        kb = (st.st_size / 1024) if st else 0.0
        if action.upper() == "DELETE":
            n_del += 1
            kb_del += kb
        else:
            n_keep += 1
            kb_keep += kb
    return n_del, kb_del, n_keep, kb_keep


def print_summary(actions: list[tuple[str, str]], n_groups: int | None = None) -> None:
    n_del, kb_del, n_keep, kb_keep = _tally(actions)
    if n_groups is not None:
        print(f"  Duplicate groups:  {n_groups}")
    print(f"  Marked DELETE:     {n_del} file(s)  →  {kb_del:.1f} KB reclaimable")
    print(f"  Marked keep:       {n_keep} file(s)  →  {kb_keep:.1f} KB retained")
    print(f"  Total on disk:     {n_del + n_keep} file(s)  →  {kb_del + kb_keep:.1f} KB")


# ── Trash ─────────────────────────────────────────────────────────────────────

def trash_files(actions: list[tuple[str, str]]) -> tuple[int, int, float]:
    """Trash all DELETE entries. Returns (trashed, failed, kb_freed)."""
    trashed = failed = 0
    kb_freed = 0.0

    for action, path_str in actions:
        if action.upper() != "DELETE":
            continue

        p = Path(path_str)
        if not p.exists():
            vprint(f"  [skip – not found] {path_str}", NORMAL)
            failed += 1
            continue

        st = _stat_safe(p)
        kb = (st.st_size / 1024) if st else 0.0

        try:
            subprocess.run(["gio", "trash", str(p)], check=True, capture_output=True)
            vprint(f"  [trashed] {path_str}", VERBOSE)
            trashed += 1
            kb_freed += kb
        except subprocess.CalledProcessError as exc:
            err = exc.stderr.decode(errors="replace").strip()
            eprint(f"  [failed]  {path_str}: {err}")
            failed += 1

    return trashed, failed, kb_freed


# ── CLI ───────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tdupes",
        description=(
            "tdupes — Find and manage duplicate files.\n\n"
            "Scans paths with fdupes, produces a TSV for review, then trashes\n"
            "files marked DELETE via 'gio trash' (recoverable until bin is emptied).\n"
            "\n"
            "Default Action logic\n"
            "  Exact-duplicate groups (byte-identical per fdupes):\n"
            "    keep   — file is in a preferred_directories folder  [comment: in preferred folder]\n"
            "    keep   — last file in the group (tiebreaker)        [comment: last in group]\n"
            "    DELETE — everything else\n"
            "    Note: CLI argument files are listed first so they are never the\n"
            "          last-in-group tiebreaker and are therefore DELETE by default.\n"
            "\n"
            "  Near-duplicate groups (-L, same basename, not byte-identical):\n"
            "    keep   — file is in a preferred_directories folder  [comment: in preferred folder]\n"
            "    keep   — overall largest, only if not already preferred  [comment: largest in basename group]\n"
            "    keep   — overall newest,  only if not already preferred  [comment: newest in basename group]\n"
            "    DELETE — everything else (CLI argument files go first; may be DELETE'd)\n"
            "    Note: if a preferred file is already the largest (or newest) across\n"
            "          the whole group, no extra non-preferred copy is kept for that reason.\n"
            "\n"
            "The Comment column in the TSV explains each keep decision.\n"
            "You can override any Action cell before confirming execution."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  tdupes ~/Pictures ~/Downloads\n"
            "  tdupes -l ~/Downloads/photo.jpg ~/Pictures\n"
            "  tdupes -L ~/Downloads/photo.jpg ~/Pictures   # include near-dupes\n"
            "  tdupes -b ~/Documents                        # batch mode, no prompts\n"
            "  tdupes -t /tmp/review.tsv ~/Music\n"
            "\n"
            "Config: $XDG_CONFIG_HOME/tdupes.yml  (created automatically on first run)\n"
        ),
    )

    parser.add_argument(
        "paths",
        nargs="*",
        metavar="PATH",
        help="Files or directories to scan for duplicates",
    )
    parser.add_argument(
        "-l", "--locate",
        action="store_true",
        help="Use locatedb to find extra candidates matching the basename of each file argument",
    )
    parser.add_argument(
        "-L", "--locate-all",
        action="store_true",
        dest="locate_all",
        help=(
            "Like -l, but also tabulate locate matches that are not byte-identical "
            "to the CLI file (near-duplicates). Among non-preferred-dir files in each "
            "near-dupe group, keeps both the largest and the newest; deletes the rest."
        ),
    )
    parser.add_argument(
        "-t", "--tsv",
        metavar="FILE",
        help="Path for the output TSV (default: temp file, or tsv_output in config)",
    )
    parser.add_argument(
        "-b", "--batch",
        action="store_true",
        help="Batch mode: no interactive prompts; DELETE actions execute immediately",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Increase output verbosity",
    )
    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Reduce output verbosity",
    )
    parser.add_argument(
        "-c", "--config",
        metavar="FILE",
        help="Path to config file (default: $XDG_CONFIG_HOME/tdupes.yml)",
    )
    parser.add_argument(
        "-V", "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    parser.add_argument(
        "-p", "--prefer",
        metavar="DIR",
        action="append",
        help=(
            "Mark DIR as a preferred directory at runtime; files inside are never "
            "proposed for deletion.  Additive with preferred_directories in config.  "
            "May be repeated: -p ~/Pictures -p ~/Archive"
        ),
    )
    parser.add_argument(
        "-x", "--exclude",
        metavar="PATTERN",
        action="append",
        help=(
            "Shell glob pattern to exclude from results at runtime.  "
            "Matched against each file's full path.  "
            "Additive with exclusion_patterns in config.  "
            "May be repeated: -x '*.tmp' -x '/mnt/*'"
        ),
    )

    return parser


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    global _verbosity

    parser = build_parser()
    args = parser.parse_args()

    if args.verbose:
        _verbosity = VERBOSE
    elif args.quiet:
        _verbosity = QUIET

    if not args.paths:
        parser.print_help()
        sys.exit(0)

    # Dependency check (before loading config so failures are early)
    missing = check_dependencies(args.locate or args.locate_all)
    if missing:
        eprint("tdupes: missing dependencies:")
        for dep in missing:
            eprint(f"  • {dep}")
        sys.exit(1)

    # Config
    config_override = Path(args.config) if args.config else None
    cfg = load_config(config_override)

    # Config may set verbosity; CLI flags take priority (already set above).
    if not args.verbose and not args.quiet:
        _verbosity = cfg.get("verbosity", NORMAL)

    batch_mode: bool = args.batch or bool(cfg.get("batch_mode", False))
    preferred_dirs: list[str] = list(cfg.get("preferred_directories") or []) + list(args.prefer or [])
    exclusion_patterns: list[str] = list(cfg.get("exclusion_patterns") or []) + list(args.exclude or [])

    # Resolve input paths
    input_paths: list[Path] = []
    cli_files: set[Path] = set()

    for raw in args.paths:
        p = Path(raw).resolve()
        if not p.exists():
            eprint(f"Warning: path not found, skipping: {raw}")
            continue
        input_paths.append(p)
        if p.is_file():
            cli_files.add(p)

    if not input_paths:
        eprint("No valid paths to scan.")
        sys.exit(1)

    # -l / -L: expand file arguments via locatedb (single batched subprocess call)
    locate_map: dict[Path, list[Path]] = {}
    if args.locate or args.locate_all:
        vprint("Searching locatedb for files with matching basenames…", NORMAL)
        located_extra: list[Path] = []
        all_located = locate_by_basenames(cli_files)
        for cf in sorted(cli_files):
            siblings = all_located.get(cf, [])
            vprint(f"  {cf.name}: {len(siblings)} candidate(s) found", VERBOSE)
            if args.locate_all:
                locate_map[cf] = siblings
            for s in siblings:
                if s not in input_paths:
                    input_paths.append(s)
                    located_extra.append(s)
        if located_extra:
            vprint(f"  Added {len(located_extra)} path(s) from locatedb.", NORMAL)

    vprint(f"Scanning {len(input_paths)} path(s) with fdupes…", NORMAL)

    # fdupes
    fdupes_groups = run_fdupes(input_paths)
    fdupes_groups = apply_exclusions(fdupes_groups, exclusion_patterns)

    # -L: build near-dupe groups for locate matches that aren't exact duplicates
    near_dupe_groups: list[list[FileEntry]] = []
    if args.locate_all and locate_map:
        near_dupe_groups = build_near_dupe_groups(
            cli_files, locate_map, fdupes_groups, preferred_dirs
        )

    if not fdupes_groups and not near_dupe_groups:
        vprint("No duplicates found.", NORMAL)
        sys.exit(0)

    if fdupes_groups:
        vprint(f"Found {len(fdupes_groups)} exact-duplicate group(s).", NORMAL)
    if near_dupe_groups:
        vprint(f"Found {len(near_dupe_groups)} near-duplicate group(s).", NORMAL)

    # Build TSV model for exact duplicates
    tsv_groups = build_tsv_groups(fdupes_groups, cli_files, preferred_dirs)

    # Determine TSV path
    tmp_path: str | None = None
    if args.tsv:
        tsv_path = Path(args.tsv)
    elif cfg.get("tsv_output"):
        tsv_path = Path(cfg["tsv_output"])
    else:
        fd, tmp_name = tempfile.mkstemp(suffix=".tsv", prefix="tdupes_")
        os.close(fd)
        tsv_path = Path(tmp_name)
        tmp_path = tmp_name

    write_tsv(tsv_groups, tsv_path, near_dupe_groups=near_dupe_groups or None)
    vprint(f"TSV written to: {tsv_path}", NORMAL)

    # Print tables to terminal
    if _verbosity >= NORMAL:
        print()
        if tsv_groups:
            print_tsv_table(tsv_groups, title="Exact duplicates:")
        if near_dupe_groups:
            print_tsv_table(near_dupe_groups, title="\nNear-duplicates (same basename, not byte-identical):")
        print()

    all_actions = read_tsv(tsv_path)

    n_total_groups = len(tsv_groups) + len(near_dupe_groups)
    print("── Initial summary ──────────────────────────────────────────")
    print_summary(all_actions, n_groups=n_total_groups)
    print()

    # ── Batch mode ────────────────────────────────────────────────────────────
    if batch_mode:
        vprint("Batch mode: executing actions without interaction.", NORMAL)
        trashed, failed, kb_freed = trash_files(all_actions)
        print("── Outcome ──────────────────────────────────────────────────")
        print(f"  Trashed:  {trashed} file(s)  →  {kb_freed:.1f} KB freed")
        if failed:
            print(f"  Failed:   {failed} file(s)  (see stderr)")
        print(f"  TSV log:  {tsv_path}")
        return

    # ── Interactive mode ──────────────────────────────────────────────────────
    print(f"Opening TSV for review: {tsv_path}")
    print("Edit the Action column (DELETE / keep) as needed, then save and return here.")
    try:
        subprocess.Popen(["xdg-open", str(tsv_path)])
    except Exception as exc:
        eprint(f"Warning: could not open TSV with xdg-open: {exc}")

    input("\nPress Enter when you have finished editing the TSV… ")

    # Re-read after edits (use whatever paths/actions the user left in the file)
    all_actions = read_tsv(tsv_path)

    # Re-parse the edited TSV into display groups and print them
    if _verbosity >= NORMAL:
        edited_exact, edited_near = _parse_tsv_for_display(tsv_path)
        print()
        if edited_exact:
            print_tsv_table(edited_exact, title="Exact duplicates (after edits):")
        if edited_near:
            print_tsv_table(edited_near, title="\nNear-duplicates (after edits):")

    print()
    print("── Updated plan ─────────────────────────────────────────────")
    print_summary(all_actions, n_groups=n_total_groups)
    print()

    answer = input("Execute these actions? [yes/no]: ").strip().lower()
    if answer not in ("yes", "y"):
        print("Aborted. No files were modified.")
        if tmp_path:
            print(f"TSV is at: {tsv_path}")
        return

    trashed, failed, kb_freed = trash_files(all_actions)

    print()
    print("── Outcome ──────────────────────────────────────────────────")
    print(f"  Trashed:  {trashed} file(s)  →  {kb_freed:.1f} KB freed")
    if failed:
        print(f"  Failed:   {failed} file(s)  (see messages above)")
    print(f"  TSV:      {tsv_path}")
    if not failed:
        print("\nAll done. Deleted files are in the trash and can be recovered until emptied.")


def entry_point() -> None:
    main()


if __name__ == "__main__":
    main()
