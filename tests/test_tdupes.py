"""
Tests for tdupes.

Unit tests cover the library functions directly.
E2E tests run `python -m tdupes` as a subprocess against real temp files
and verify observable side-effects (files trashed, TSV content, exit codes).
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
import yaml

from tdupes.__main__ import (
    DEFAULT_CONFIG,
    _default_system_preferred,
    _fsize,
    _in_preferred,
    _mtime,
    _sim_flagged_delete,
    _tally,
    apply_exclusions,
    build_near_dupe_groups,
    build_tsv_groups,
    load_config,
    read_tsv,
    similarity_code,
    write_tsv,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_file(path: Path, content: str = "hello", mtime: float | None = None) -> Path:
    path.write_text(content)
    if mtime is not None:
        os.utime(path, (mtime, mtime))
    return path


def run_tdupes(*args: str, config: Path) -> subprocess.CompletedProcess:
    """Run tdupes as a subprocess with an isolated config file."""
    cmd = [sys.executable, "-m", "tdupes", "--config", str(config)] + list(args)
    return subprocess.run(cmd, capture_output=True, text=True)


@pytest.fixture
def cfg(tmp_path: Path) -> Path:
    """Minimal isolated config file pointing nowhere special."""
    p = tmp_path / "tdupes.yml"
    p.write_text(yaml.dump(dict(DEFAULT_CONFIG)))
    return p


# ── Unit: load_config ─────────────────────────────────────────────────────────

def test_load_config_creates_default(tmp_path: Path):
    path = tmp_path / "new.yml"
    result = load_config(path)
    assert path.exists(), "config file should be created"
    # On first run preferred_directories is pre-populated with system dirs
    assert isinstance(result["preferred_directories"], list)
    assert "/home" not in result["preferred_directories"]
    assert "/tmp" not in result["preferred_directories"]
    assert result["batch_mode"] is False


def test_load_config_reads_values(tmp_path: Path):
    path = tmp_path / "cfg.yml"
    path.write_text(yaml.dump({"preferred_directories": ["/keep"], "verbosity": 2}))
    result = load_config(path)
    assert result["preferred_directories"] == ["/keep"]
    assert result["verbosity"] == 2
    assert result["exclusion_patterns"] == []  # default preserved


def test_load_config_ignores_unknown_keys(tmp_path: Path):
    path = tmp_path / "cfg.yml"
    path.write_text(yaml.dump({"bogus_key": "value"}))
    result = load_config(path)
    assert "bogus_key" not in result


# ── Unit: _in_preferred ───────────────────────────────────────────────────────

def test_in_preferred_direct_child(tmp_path: Path):
    pref = tmp_path / "pref"
    pref.mkdir()
    f = make_file(pref / "file.txt")
    assert _in_preferred(f, [str(pref)]) is True


def test_in_preferred_nested_subdir(tmp_path: Path):
    pref = tmp_path / "pref"
    deep = pref / "a" / "b"
    deep.mkdir(parents=True)
    f = make_file(deep / "file.txt")
    assert _in_preferred(f, [str(pref)]) is True


def test_in_preferred_different_dir(tmp_path: Path):
    pref = tmp_path / "pref"
    other = tmp_path / "other"
    other.mkdir()
    f = make_file(other / "file.txt")
    assert _in_preferred(f, [str(pref)]) is False


def test_in_preferred_empty_list(tmp_path: Path):
    f = make_file(tmp_path / "file.txt")
    assert _in_preferred(f, []) is False


def test_in_preferred_multiple_prefs_one_matches(tmp_path: Path):
    pref_a = tmp_path / "a"
    pref_b = tmp_path / "b"
    pref_a.mkdir()
    pref_b.mkdir()
    f = make_file(pref_b / "file.txt")
    assert _in_preferred(f, [str(pref_a), str(pref_b)]) is True


# ── Unit: apply_exclusions ────────────────────────────────────────────────────

def test_apply_exclusions_empty_patterns_unchanged(tmp_path: Path):
    a = make_file(tmp_path / "a.txt")
    b = make_file(tmp_path / "b.txt")
    groups = [[a, b]]
    assert apply_exclusions(groups, []) == groups


def test_apply_exclusions_removes_matching_file(tmp_path: Path):
    a = make_file(tmp_path / "a.txt")
    b = make_file(tmp_path / "b.log")
    c = make_file(tmp_path / "c.txt")
    result = apply_exclusions([[a, b, c]], ["*.log"])
    assert len(result) == 1
    assert b not in result[0]
    assert a in result[0] and c in result[0]


def test_apply_exclusions_drops_group_reduced_to_one(tmp_path: Path):
    a = make_file(tmp_path / "a.txt")
    b = make_file(tmp_path / "b.log")
    # Excluding b leaves only a — not a duplicate group anymore
    result = apply_exclusions([[a, b]], ["*.log"])
    assert result == []


def test_apply_exclusions_full_path_pattern(tmp_path: Path):
    sub = tmp_path / "cache"
    sub.mkdir()
    a = make_file(sub / "file.txt")
    b = make_file(tmp_path / "file.txt")
    result = apply_exclusions([[a, b]], [str(sub / "*")])
    assert a not in (result[0] if result else [])


# ── Unit: build_tsv_groups ────────────────────────────────────────────────────

def test_build_tsv_groups_cli_file_goes_first(tmp_path: Path):
    cli = make_file(tmp_path / "cli.txt", "x")
    other = make_file(tmp_path / "other.txt", "x")
    groups = build_tsv_groups([[other, cli]], cli_files={cli}, preferred=[])
    assert groups[0][0].path == cli


def test_build_tsv_groups_mtime_newest_first_oldest_last(tmp_path: Path):
    now = time.time()
    newest = make_file(tmp_path / "newest.txt", "x", mtime=now)
    middle = make_file(tmp_path / "middle.txt", "x", mtime=now - 100)
    oldest = make_file(tmp_path / "oldest.txt", "x", mtime=now - 200)
    groups = build_tsv_groups(
        [[oldest, newest, middle]], cli_files=set(), preferred=[]
    )
    paths = [e.path for e in groups[0]]
    assert paths[0] == newest
    assert paths[-1] == oldest


def test_build_tsv_groups_last_entry_is_keep(tmp_path: Path):
    files = [make_file(tmp_path / f"{i}.txt", "x") for i in range(3)]
    groups = build_tsv_groups([files], cli_files=set(), preferred=[])
    actions = [e.action for e in groups[0]]
    assert actions[-1] == "keep"
    assert all(a == "DELETE" for a in actions[:-1])


def test_build_tsv_groups_preferred_dir_is_keep(tmp_path: Path):
    pref_dir = tmp_path / "important"
    pref_dir.mkdir()
    pref_file = make_file(pref_dir / "file.txt", "x")
    other = make_file(tmp_path / "other.txt", "x")
    groups = build_tsv_groups(
        [[other, pref_file]], cli_files=set(), preferred=[str(pref_dir)]
    )
    by_path = {e.path: e.action for e in groups[0]}
    assert by_path[pref_file] == "keep"


def test_build_tsv_groups_all_preferred_all_keep(tmp_path: Path):
    pref = tmp_path / "pref"
    pref.mkdir()
    a = make_file(pref / "a.txt", "x")
    b = make_file(pref / "b.txt", "x")
    groups = build_tsv_groups([[a, b]], cli_files=set(), preferred=[str(pref)])
    assert all(e.action == "keep" for e in groups[0])


def test_build_tsv_groups_skips_single_file_group(tmp_path: Path):
    f = make_file(tmp_path / "solo.txt", "x")
    assert build_tsv_groups([[f]], cli_files=set(), preferred=[]) == []


def test_build_tsv_groups_similarity_is_100(tmp_path: Path):
    a = make_file(tmp_path / "a.txt", "x")
    b = make_file(tmp_path / "b.txt", "x")
    groups = build_tsv_groups([[a, b]], cli_files=set(), preferred=[])
    assert all(e.similarity == "100" for e in groups[0])


# ── Unit: write_tsv / read_tsv ────────────────────────────────────────────────

def test_write_read_roundtrip(tmp_path: Path):
    a = make_file(tmp_path / "a.txt", "x")
    b = make_file(tmp_path / "b.txt", "x")
    groups = build_tsv_groups([[a, b]], cli_files=set(), preferred=[])
    tsv = tmp_path / "out.tsv"
    write_tsv(groups, tsv)
    rows = read_tsv(tsv)
    assert len(rows) == 2
    paths_in_tsv = {Path(p) for _, p in rows}
    assert a in paths_in_tsv and b in paths_in_tsv


def test_read_tsv_skips_header_and_blank_lines(tmp_path: Path):
    tsv = tmp_path / "t.tsv"
    tsv.write_text(
        "Action\tSimilarity\tSize_KB\tModified\tPath\n"
        "\n"
        "DELETE\t100\t1.0\t2024-01-01T00:00:00\t/foo/a.txt\n"
        "\n"
        "keep\t100\t1.0\t2024-01-01T00:00:00\t/foo/b.txt\n"
    )
    rows = read_tsv(tsv)
    assert rows == [("DELETE", "/foo/a.txt"), ("keep", "/foo/b.txt")]


def test_read_tsv_after_user_edits(tmp_path: Path):
    a = make_file(tmp_path / "a.txt", "x")
    b = make_file(tmp_path / "b.txt", "x")
    groups = build_tsv_groups([[a, b]], cli_files=set(), preferred=[])
    tsv = tmp_path / "out.tsv"
    write_tsv(groups, tsv)

    # Simulate user overriding every DELETE → keep
    original = tsv.read_text()
    tsv.write_text(original.replace("DELETE", "keep"))

    rows = read_tsv(tsv)
    assert all(action == "keep" for action, _ in rows)


def test_write_tsv_multiple_groups_separated_by_blank_line(tmp_path: Path):
    a = make_file(tmp_path / "a.txt", "x")
    b = make_file(tmp_path / "b.txt", "x")
    c = make_file(tmp_path / "c.txt", "y")
    d = make_file(tmp_path / "d.txt", "y")
    groups = build_tsv_groups([[a, b], [c, d]], cli_files=set(), preferred=[])
    tsv = tmp_path / "out.tsv"
    write_tsv(groups, tsv)
    # Blank line between groups (after the header)
    body_lines = tsv.read_text().splitlines()[1:]  # skip header
    blank_lines = [l for l in body_lines if l == ""]
    assert len(blank_lines) == 1


# ── Unit: similarity_code ─────────────────────────────────────────────────────

def test_similarity_text_identical(tmp_path: Path):
    a = make_file(tmp_path / "a.txt", "hello world\n")
    b = make_file(tmp_path / "b.txt", "hello world\n")
    assert similarity_code(a, b) == "100"


def test_similarity_text_partial_match(tmp_path: Path):
    base = "hello world\n" * 20
    a = make_file(tmp_path / "a.txt", base)
    b = make_file(tmp_path / "b.txt", base[:len(base) // 2] + "XXXXX\n" * 20)
    code = similarity_code(a, b)
    assert code not in ("100", "XXX", "!!!")
    assert code.isdigit()
    assert 0 <= int(code) < 100


def test_similarity_binary_same_size(tmp_path: Path):
    a = tmp_path / "a.bin"
    b = tmp_path / "b.bin"
    a.write_bytes(b"\x00\x01\x02\x03")
    b.write_bytes(b"\x00\x04\x05\x06")
    assert similarity_code(a, b) == "XXX"


def test_similarity_binary_different_size(tmp_path: Path):
    a = tmp_path / "a.bin"
    b = tmp_path / "b.bin"
    a.write_bytes(b"\x00\x01\x02\x03")
    b.write_bytes(b"\x00\x01")
    assert similarity_code(a, b) == "!!!"


# ── Unit: _tally ──────────────────────────────────────────────────────────────

def test_tally_counts_and_sizes(tmp_path: Path):
    a = make_file(tmp_path / "a.txt", "x" * 1024)
    b = make_file(tmp_path / "b.txt", "y" * 2048)
    n_del, kb_del, n_keep, kb_keep = _tally([("DELETE", str(a)), ("keep", str(b))])
    assert n_del == 1 and n_keep == 1
    assert kb_del > 0 and kb_keep > kb_del


def test_tally_missing_file_counts_zero_kb(tmp_path: Path):
    n_del, kb_del, _, _ = _tally([("DELETE", str(tmp_path / "ghost.txt"))])
    assert n_del == 1
    assert kb_del == 0.0


def test_tally_case_insensitive_action(tmp_path: Path):
    f = make_file(tmp_path / "f.txt", "x")
    n_del, _, _, _ = _tally([("delete", str(f))])
    assert n_del == 1


# ── E2E tests ─────────────────────────────────────────────────────────────────

def test_e2e_no_args_prints_help(tmp_path: Path, cfg: Path):
    result = run_tdupes(config=cfg)
    assert result.returncode == 0
    assert "usage:" in result.stdout.lower()


def test_e2e_no_duplicates_exits_cleanly(tmp_path: Path, cfg: Path):
    make_file(tmp_path / "a.txt", "alpha")
    make_file(tmp_path / "b.txt", "beta")
    result = run_tdupes("--batch", str(tmp_path), config=cfg)
    assert result.returncode == 0
    assert "no duplicates" in result.stdout.lower()


def test_e2e_batch_trashes_one_of_two_dupes(tmp_path: Path, cfg: Path):
    content = "identical content for e2e duplicate test"
    a = make_file(tmp_path / "a.txt", content)
    b = make_file(tmp_path / "b.txt", content)

    result = run_tdupes("--batch", "--quiet", str(tmp_path), config=cfg)
    assert result.returncode == 0

    still_alive = [f for f in (a, b) if f.exists()]
    assert len(still_alive) == 1, "exactly one of the two dupes should be trashed"


def test_e2e_batch_tsv_written_to_specified_path(tmp_path: Path, cfg: Path):
    tsv_path = tmp_path / "report.tsv"
    make_file(tmp_path / "a.txt", "same")
    make_file(tmp_path / "b.txt", "same")
    run_tdupes("--batch", "--tsv", str(tsv_path), str(tmp_path), config=cfg)
    assert tsv_path.exists()
    rows = read_tsv(tsv_path)
    assert len(rows) == 2


def test_e2e_preferred_dir_file_not_trashed(tmp_path: Path, cfg: Path):
    pref_dir = tmp_path / "important"
    pref_dir.mkdir()
    content = "precious file content"
    pref_file = make_file(pref_dir / "file.txt", content)
    other_file = make_file(tmp_path / "copy.txt", content)

    cfg.write_text(yaml.dump({**DEFAULT_CONFIG, "preferred_directories": [str(pref_dir)]}))
    tsv_path = tmp_path / "out.tsv"
    run_tdupes("--batch", "--tsv", str(tsv_path), str(tmp_path), config=cfg)

    assert pref_file.exists(), "file in preferred_directories must never be trashed"
    rows = read_tsv(tsv_path)
    by_path = {p: a for a, p in rows}
    assert by_path.get(str(pref_file)) == "keep"


def test_e2e_exclusion_pattern_omits_files(tmp_path: Path, cfg: Path):
    content = "same content"
    make_file(tmp_path / "a.txt", content)
    make_file(tmp_path / "b.txt", content)
    make_file(tmp_path / "c.bak", content)

    cfg.write_text(yaml.dump({**DEFAULT_CONFIG, "exclusion_patterns": ["*.bak"]}))
    tsv_path = tmp_path / "out.tsv"
    run_tdupes("--batch", "--tsv", str(tsv_path), str(tmp_path), config=cfg)

    assert tsv_path.exists()
    rows = read_tsv(tsv_path)
    paths = [p for _, p in rows]
    assert not any(p.endswith(".bak") for p in paths)


def test_e2e_nonexistent_path_warns_and_continues(tmp_path: Path, cfg: Path):
    ghost = str(tmp_path / "does_not_exist")
    real_a = make_file(tmp_path / "a.txt", "x")
    real_b = make_file(tmp_path / "b.txt", "x")  # noqa: F841 (needed for fdupes to have 2 files)
    result = run_tdupes("--batch", ghost, str(tmp_path), config=cfg)
    combined = result.stdout + result.stderr
    assert "warning" in combined.lower()


def test_e2e_quiet_produces_less_output_than_normal(tmp_path: Path, cfg: Path):
    content = "same"
    make_file(tmp_path / "a.txt", content)
    make_file(tmp_path / "b.txt", content)

    normal = run_tdupes("--batch", str(tmp_path), config=cfg)
    quiet  = run_tdupes("--batch", "--quiet", str(tmp_path), config=cfg)
    assert len(quiet.stdout) < len(normal.stdout)


def test_e2e_verbose_produces_more_output_than_normal(tmp_path: Path, cfg: Path):
    content = "same"

    dir_n = tmp_path / "normal"
    dir_n.mkdir()
    make_file(dir_n / "a.txt", content)
    make_file(dir_n / "b.txt", content)
    normal = run_tdupes("--batch", str(dir_n), config=cfg)

    dir_v = tmp_path / "verbose"
    dir_v.mkdir()
    make_file(dir_v / "a.txt", content)
    make_file(dir_v / "b.txt", content)
    verbose = run_tdupes("--batch", "--verbose", str(dir_v), config=cfg)

    assert len(verbose.stdout) > len(normal.stdout)


def test_e2e_tsv_contains_correct_columns(tmp_path: Path, cfg: Path):
    tsv_path = tmp_path / "out.tsv"
    make_file(tmp_path / "a.txt", "dup")
    make_file(tmp_path / "b.txt", "dup")
    run_tdupes("--batch", "--tsv", str(tsv_path), str(tmp_path), config=cfg)

    lines = tsv_path.read_text().splitlines()
    assert lines[0] == "Action\tSimilarity\tSize_KB\tModified\tPath\tComment"
    data_lines = [l for l in lines[1:] if l.strip() and not l.startswith("#")]
    assert len(data_lines) == 2
    for line in data_lines:
        cols = line.split("\t")
        assert len(cols) == 6
        assert cols[0] in ("DELETE", "keep")
        assert cols[1] == "100"
        assert "T" in cols[3]   # ISO-8601 datetime
        assert cols[4].startswith("/")  # absolute path


def test_comment_column_exact_dupes(tmp_path: Path, cfg: Path):
    """Comment column is populated with keep-reason tags for exact-dupe groups."""
    tsv_path = tmp_path / "out.tsv"
    make_file(tmp_path / "a.txt", "dup")
    make_file(tmp_path / "b.txt", "dup")
    run_tdupes("--batch", "--tsv", str(tsv_path), str(tmp_path), config=cfg)

    lines = [l for l in tsv_path.read_text().splitlines()
             if l.strip() and not l.startswith("#") and not l.startswith("Action")]
    comments = {l.split("\t")[4]: l.split("\t")[5] for l in lines}  # path → comment

    keep_comments = [c for c in comments.values() if c]
    delete_comments = [c for c in comments.values() if not c]

    assert len(keep_comments) == 1      # kept file has a reason
    assert len(delete_comments) == 1    # deleted file has no comment
    assert "last in group" in keep_comments[0]


def test_comment_column_preferred_folder(tmp_path: Path, cfg: Path):
    pref_dir = tmp_path / "pref"
    pref_dir.mkdir()
    cfg.write_text(yaml.dump({**DEFAULT_CONFIG, "preferred_directories": [str(pref_dir)]}))
    tsv_path = tmp_path / "out.tsv"
    make_file(pref_dir / "f.txt", "dup")
    make_file(tmp_path / "g.txt", "dup")
    run_tdupes("--batch", "--tsv", str(tsv_path), str(tmp_path), config=cfg)

    lines = [l for l in tsv_path.read_text().splitlines()
             if l.strip() and not l.startswith("#") and not l.startswith("Action")]
    by_path = {l.split("\t")[4]: l.split("\t")[5] for l in lines}
    pref_comment = by_path[str(pref_dir / "f.txt")]
    assert "in preferred folder" in pref_comment


def test_comment_column_cli_file_has_no_comment_when_deleted(tmp_path: Path):
    """CLI files are listed first (not last) so they get DELETE with no comment."""
    cli   = make_file(tmp_path / "cli.txt",   "dup")
    other = make_file(tmp_path / "other.txt", "dup")
    groups = build_tsv_groups([[cli, other]], cli_files={cli}, preferred=[])
    by_path = {e.path: e for e in groups[0]}
    assert by_path[cli].action == "DELETE"
    assert by_path[cli].comment == ""          # no keep reason → no comment
    assert by_path[other].action == "keep"
    assert "last in group" in by_path[other].comment


def test_comment_column_near_dupe_default_no_comment(tmp_path: Path):
    """Without flags, all basename matches are kept and have no comment."""
    cli   = make_file(tmp_path / "cli.txt",   "a" * 10)
    other = make_file(tmp_path / "other.txt", "b" * 300)

    groups = build_near_dupe_groups(
        cli_files={cli},
        locate_map={cli: [other]},
        fdupes_groups=[],
        preferred=[],
    )
    for e in groups[0]:
        assert e.action == "keep"
        assert e.comment == ""


def test_comment_column_near_dupe_preferred_comment(tmp_path: Path):
    """Preferred files get 'in preferred folder' comment; others get no comment."""
    pref_dir = tmp_path / "pref"
    pref_dir.mkdir()
    pref_file = make_file(pref_dir / "f.txt", "x" * 10)
    other     = make_file(tmp_path / "other.txt", "y" * 50)

    groups = build_near_dupe_groups(
        cli_files={pref_file},
        locate_map={pref_file: [other]},
        fdupes_groups=[],
        preferred=[str(pref_dir)],
    )
    by_path = {e.path: e for e in groups[0]}
    assert by_path[pref_file].comment == "in preferred folder"
    assert by_path[other].comment == ""


# ── Unit: _sim_flagged_delete ─────────────────────────────────────────────────

def test_sim_flagged_delete_xxx():
    assert _sim_flagged_delete("XXX", delete_xxx=True,  delete_nnn=False, delete_excl=False)
    assert not _sim_flagged_delete("XXX", delete_xxx=False, delete_nnn=False, delete_excl=False)


def test_sim_flagged_delete_nnn():
    assert _sim_flagged_delete("075", delete_xxx=False, delete_nnn=True,  delete_excl=False)
    assert _sim_flagged_delete("000", delete_xxx=False, delete_nnn=True,  delete_excl=False)
    assert not _sim_flagged_delete("075", delete_xxx=False, delete_nnn=False, delete_excl=False)


def test_sim_flagged_delete_excl():
    assert _sim_flagged_delete("!!!", delete_xxx=False, delete_nnn=False, delete_excl=True)
    assert not _sim_flagged_delete("!!!", delete_xxx=False, delete_nnn=False, delete_excl=False)


def test_sim_flagged_delete_100_never_deleted():
    """Similarity '100' (exact match reference) is never flagged for deletion."""
    assert not _sim_flagged_delete("100", delete_xxx=True, delete_nnn=True, delete_excl=True)


# ── Unit: build_near_dupe_groups ──────────────────────────────────────────────

def test_build_near_dupe_groups_basic(tmp_path: Path):
    cli = make_file(tmp_path / "cli.txt", "hello")
    near = make_file(tmp_path / "near.txt", "world")  # different content = near-dupe

    groups = build_near_dupe_groups(
        cli_files={cli},
        locate_map={cli: [near]},
        fdupes_groups=[],   # no exact dupes
        preferred=[],
    )
    assert len(groups) == 1
    paths = [e.path for e in groups[0]]
    assert paths[0] == cli   # CLI file always first
    assert near in paths


def test_build_near_dupe_groups_excludes_exact_dupes(tmp_path: Path):
    """Locate matches that are already in the same fdupes group are excluded."""
    cli   = make_file(tmp_path / "cli.txt", "x")
    exact = make_file(tmp_path / "exact.txt", "x")   # exact dupe of cli
    near  = make_file(tmp_path / "near.txt", "y")    # different

    groups = build_near_dupe_groups(
        cli_files={cli},
        locate_map={cli: [exact, near]},
        fdupes_groups=[[cli, exact]],
        preferred=[],
    )
    assert len(groups) == 1
    paths = [e.path for e in groups[0]]
    assert exact not in paths
    assert near in paths


def test_build_near_dupe_groups_no_matches_returns_empty(tmp_path: Path):
    cli = make_file(tmp_path / "cli.txt", "x")
    groups = build_near_dupe_groups(
        cli_files={cli},
        locate_map={cli: []},
        fdupes_groups=[],
        preferred=[],
    )
    assert groups == []


def test_build_near_dupe_groups_all_are_exact_dupes_returns_empty(tmp_path: Path):
    cli   = make_file(tmp_path / "cli.txt", "x")
    exact = make_file(tmp_path / "exact.txt", "x")
    groups = build_near_dupe_groups(
        cli_files={cli},
        locate_map={cli: [exact]},
        fdupes_groups=[[cli, exact]],
        preferred=[],
    )
    assert groups == []


def test_build_near_dupe_groups_similarity_code_not_100(tmp_path: Path):
    """Non-identical locate matches get a real similarity code, not '100'."""
    cli  = make_file(tmp_path / "cli.txt",  "hello world\n")
    near = make_file(tmp_path / "near.txt", "goodbye world\n")

    groups = build_near_dupe_groups(
        cli_files={cli},
        locate_map={cli: [near]},
        fdupes_groups=[],
        preferred=[],
    )
    by_path = {e.path: e for e in groups[0]}
    assert by_path[cli].similarity == "100"      # reference always "100"
    assert by_path[near].similarity != "100"     # near-dupe gets real code


def test_build_near_dupe_groups_default_all_kept(tmp_path: Path):
    """Without flags, all files in a basename match group are kept."""
    now = time.time()
    cli   = make_file(tmp_path / "cli.txt",   "a" * 10,  mtime=now - 200)
    large = make_file(tmp_path / "large.txt", "b" * 300, mtime=now - 100)
    new_  = make_file(tmp_path / "new.txt",   "c" * 50,  mtime=now)

    groups = build_near_dupe_groups(
        cli_files={cli},
        locate_map={cli: [large, new_]},
        fdupes_groups=[],
        preferred=[],
    )
    by_path = {e.path: e.action for e in groups[0]}
    assert by_path[cli]   == "keep"
    assert by_path[large] == "keep"
    assert by_path[new_]  == "keep"


def test_build_near_dupe_groups_preferred_kept_regardless(tmp_path: Path):
    pref_dir = tmp_path / "pref"
    pref_dir.mkdir()
    pref_file = make_file(pref_dir / "cli.txt", "x" * 10)
    near      = make_file(tmp_path / "near.txt", "y" * 500)  # larger than pref

    groups = build_near_dupe_groups(
        cli_files={pref_file},
        locate_map={pref_file: [near]},
        fdupes_groups=[],
        preferred=[str(pref_dir)],
    )
    by_path = {e.path: e.action for e in groups[0]}
    assert by_path[pref_file] == "keep"   # preferred, always kept


def test_build_near_dupe_groups_preferred_comment_only(tmp_path: Path):
    """Preferred file gets 'in preferred folder'; non-preferred kept files have no comment."""
    pref_dir = tmp_path / "pref"
    pref_dir.mkdir()
    pref_file = make_file(pref_dir / "big.txt", "x" * 500)
    non_file  = make_file(tmp_path / "other.txt", "x" * 10)

    groups = build_near_dupe_groups(
        cli_files={pref_file},
        locate_map={pref_file: [non_file]},
        fdupes_groups=[],
        preferred=[str(pref_dir)],
    )
    by_path = {e.path: e for e in groups[0]}
    assert by_path[pref_file].action == "keep"
    assert by_path[pref_file].comment == "in preferred folder"
    assert by_path[non_file].action == "keep"
    assert by_path[non_file].comment == ""


def test_build_near_dupe_groups_cli_first_in_output(tmp_path: Path):
    now = time.time()
    cli   = make_file(tmp_path / "cli.txt",  "x", mtime=now - 100)
    near1 = make_file(tmp_path / "n1.txt",   "a", mtime=now)        # newer
    near2 = make_file(tmp_path / "n2.txt",   "b", mtime=now - 200)  # older

    groups = build_near_dupe_groups(
        cli_files={cli},
        locate_map={cli: [near2, near1]},
        fdupes_groups=[],
        preferred=[],
    )
    paths = [e.path for e in groups[0]]
    assert paths[0] == cli       # always first
    assert paths[1] == near1     # newest of the rest
    assert paths[2] == near2     # oldest last


# ── Unit: write_tsv with near-dupe section ────────────────────────────────────

def test_write_tsv_near_dupe_section_has_comment(tmp_path: Path):
    cli  = make_file(tmp_path / "cli.txt",  "hello")
    near = make_file(tmp_path / "near.txt", "world")

    exact_groups: list = []
    near_groups = build_near_dupe_groups(
        cli_files={cli},
        locate_map={cli: [near]},
        fdupes_groups=[],
        preferred=[],
    )
    tsv = tmp_path / "out.tsv"
    write_tsv(exact_groups, tsv, near_dupe_groups=near_groups)

    content = tsv.read_text()
    assert "BASENAME MATCHES" in content


def test_read_tsv_skips_comment_lines(tmp_path: Path):
    tsv = tmp_path / "t.tsv"
    tsv.write_text(
        "Action\tSimilarity\tSize_KB\tModified\tPath\n"
        "\n"
        "#\t\t\t\tBASENAME MATCHES — same basename, not byte-identical\n"
        "\n"
        "keep\t100\t1.0\t2024-01-01T00:00:00\t/a/file.txt\n"
    )
    rows = read_tsv(tsv)
    assert len(rows) == 1
    assert rows[0] == ("keep", "/a/file.txt")


# ── E2E: -L flag ──────────────────────────────────────────────────────────────

def test_e2e_locate_all_near_dupes_in_tsv(tmp_path: Path, cfg: Path):
    """
    Two files with the same basename but different content: with -L, both appear
    in the TSV as a near-dupe group even though fdupes won't group them.
    We fake the locate step by passing both files directly so we can test
    the -L path without needing locatedb populated.
    """
    dir_a = tmp_path / "dir_a"
    dir_b = tmp_path / "dir_b"
    dir_a.mkdir()
    dir_b.mkdir()
    cli_file  = make_file(dir_a / "report.txt", "version one content here")
    near_file = make_file(dir_b / "report.txt", "version two different content")

    tsv_path = tmp_path / "out.tsv"

    # Call build_near_dupe_groups directly (unit-level) to verify the logic
    # works end-to-end without needing a real locatedb.
    near_groups = build_near_dupe_groups(
        cli_files={cli_file},
        locate_map={cli_file: [near_file]},
        fdupes_groups=[],   # fdupes finds nothing (different content)
        preferred=[],
    )
    write_tsv([], tsv_path, near_dupe_groups=near_groups)

    rows = read_tsv(tsv_path)
    assert len(rows) == 2
    paths_in_tsv = {Path(p) for _, p in rows}
    assert cli_file in paths_in_tsv
    assert near_file in paths_in_tsv


def test_e2e_locate_all_near_dupe_similarity_codes(tmp_path: Path, cfg: Path):
    """Similarity codes for near-dupes reflect actual file relationships."""
    cli     = make_file(tmp_path / "a.txt", "some text content\n" * 10)
    text_nd = make_file(tmp_path / "b.txt", "some text content\n" * 5 + "other\n" * 5)
    bin_a   = tmp_path / "c.bin"
    bin_b   = tmp_path / "d.bin"
    bin_a.write_bytes(b"\x00\x01\x02\x03")
    bin_b.write_bytes(b"\x00\x04")  # different size

    nd_text = build_near_dupe_groups(
        cli_files={cli}, locate_map={cli: [text_nd]},
        fdupes_groups=[], preferred=[],
    )
    assert nd_text[0][1].similarity not in ("100", "!!!", "XXX")  # text → NNN

    nd_bin = build_near_dupe_groups(
        cli_files={bin_a}, locate_map={bin_a: [bin_b]},
        fdupes_groups=[], preferred=[],
    )
    assert nd_bin[0][1].similarity == "!!!"  # binary, different size


def test_e2e_locate_all_all_kept_by_default(tmp_path: Path, cfg: Path):
    """All basename matches are kept by default — including the CLI file."""
    now = time.time()
    cli_file  = make_file(tmp_path / "cli.txt",  "x" * 10,  mtime=now - 200)
    big_file  = make_file(tmp_path / "big.txt",  "x" * 500, mtime=now - 100)
    new_file  = make_file(tmp_path / "new.txt",  "x" * 50,  mtime=now)

    groups = build_near_dupe_groups(
        cli_files={cli_file},
        locate_map={cli_file: [big_file, new_file]},
        fdupes_groups=[],
        preferred=[],
    )
    by_path = {e.path: e.action for e in groups[0]}
    assert by_path[cli_file] == "keep"
    assert by_path[big_file] == "keep"
    assert by_path[new_file] == "keep"


def test_e2e_locate_all_help_shows_flag(tmp_path: Path, cfg: Path):
    result = run_tdupes("--help", config=cfg)
    assert "-L" in result.stdout or "locate-all" in result.stdout


# ── Unit: build_near_dupe_groups with -X / -N / -Z flags ─────────────────────

def test_build_near_dupe_groups_delete_xxx_flag(tmp_path: Path):
    """With delete_xxx=True, non-preferred XXX-similarity files get DELETE."""
    cli  = tmp_path / "cli.bin"
    near = tmp_path / "near.bin"
    cli.write_bytes(b"\x00\x01\x02\x03")   # binary
    near.write_bytes(b"\x04\x05\x06\x07")  # binary, same size → XXX

    groups = build_near_dupe_groups(
        cli_files={cli},
        locate_map={cli: [near]},
        fdupes_groups=[],
        preferred=[],
        delete_xxx=True,
    )
    by_path = {e.path: e.action for e in groups[0]}
    assert by_path[cli]  == "keep"    # cli ref has sim "100", never deleted
    assert by_path[near] == "DELETE"  # XXX → deleted by flag


def test_build_near_dupe_groups_delete_nnn_flag(tmp_path: Path):
    """With delete_nnn=True, non-preferred NNN-similarity text files get DELETE."""
    cli  = make_file(tmp_path / "cli.txt",  "hello world\n" * 10)
    near = make_file(tmp_path / "near.txt", "hello world\n" * 5 + "goodbye\n" * 5)

    groups = build_near_dupe_groups(
        cli_files={cli},
        locate_map={cli: [near]},
        fdupes_groups=[],
        preferred=[],
        delete_nnn=True,
    )
    by_path = {e.path: e.action for e in groups[0]}
    assert by_path[cli]  == "keep"
    assert by_path[near] == "DELETE"  # NNN% text match → deleted by flag


def test_build_near_dupe_groups_delete_excl_flag(tmp_path: Path):
    """With delete_excl=True, non-preferred !!!-similarity files get DELETE."""
    cli  = tmp_path / "cli.bin"
    near = tmp_path / "near.bin"
    cli.write_bytes(b"\x00\x01\x02\x03")        # 4 bytes
    near.write_bytes(b"\x00\x01\x02\x03\x04")   # 5 bytes → different size → !!!

    groups = build_near_dupe_groups(
        cli_files={cli},
        locate_map={cli: [near]},
        fdupes_groups=[],
        preferred=[],
        delete_excl=True,
    )
    by_path = {e.path: e.action for e in groups[0]}
    assert by_path[cli]  == "keep"
    assert by_path[near] == "DELETE"  # !!! → deleted by flag


def test_build_near_dupe_groups_preferred_overrides_delete_flag(tmp_path: Path):
    """Preferred files are never deleted even when a delete flag matches their sim code."""
    pref_dir = tmp_path / "pref"
    pref_dir.mkdir()
    pref_file = pref_dir / "f.bin"
    cli       = tmp_path / "cli.bin"
    pref_file.write_bytes(b"\x00\x01\x02\x03")
    cli.write_bytes(b"\x04\x05\x06\x07")  # binary, same size as pref → XXX

    groups = build_near_dupe_groups(
        cli_files={cli},
        locate_map={cli: [pref_file]},
        fdupes_groups=[],
        preferred=[str(pref_dir)],
        delete_xxx=True,
    )
    by_path = {e.path: e.action for e in groups[0]}
    assert by_path[pref_file] == "keep"   # preferred overrides -X


# ── Unit: _default_system_preferred ──────────────────────────────────────────

def test_default_system_preferred_excludes_home_and_tmp():
    dirs = _default_system_preferred()
    assert "/home" not in dirs
    assert "/tmp" not in dirs
    # Should include at least some system dirs on any standard Linux system
    assert len(dirs) > 0
    # All returned entries should be absolute paths under /
    for d in dirs:
        assert d.startswith("/")


# ── E2E: -r / --remove-prefer flag ───────────────────────────────────────────

def test_e2e_remove_prefer_removes_protection(tmp_path: Path, cfg: Path):
    """With -r, a preferred dir from config loses its protection for this run."""
    pref_dir = tmp_path / "important"
    pref_dir.mkdir()
    content = "same content"
    pref_file = make_file(pref_dir / "file.txt", content)
    other     = make_file(tmp_path / "other.txt", content)

    # Config sets pref_dir as preferred
    cfg.write_text(yaml.dump({**DEFAULT_CONFIG, "preferred_directories": [str(pref_dir)]}))
    tsv_path = tmp_path / "out.tsv"

    # Without -r: pref_file is kept
    run_tdupes("--batch", "--tsv", str(tsv_path), str(tmp_path), config=cfg)
    rows = read_tsv(tsv_path)
    by_path = {p: a for a, p in rows}
    assert by_path.get(str(pref_file)) == "keep"

    # With -r: protection removed, pref_file may be deleted
    tsv_path2 = tmp_path / "out2.tsv"
    run_tdupes("--batch", "--tsv", str(tsv_path2),
               "--remove-prefer", str(pref_dir),
               str(tmp_path), config=cfg)
    rows2 = read_tsv(tsv_path2)
    by_path2 = {p: a for a, p in rows2}
    # Without preferred protection, one file will be keep and one DELETE (last-in-group)
    actions = set(by_path2.values())
    assert "DELETE" in actions  # at least one file is now marked DELETE


# ── E2E: -s / -S / -r flags in --help ────────────────────────────────────────

def test_e2e_new_flags_in_help(tmp_path: Path, cfg: Path):
    result = run_tdupes("--help", config=cfg)
    assert "-s" in result.stdout or "system-prefer" in result.stdout
    assert "-S" in result.stdout or "no-system-prefer" in result.stdout
    assert "-r" in result.stdout or "remove-prefer" in result.stdout
    assert "-X" in result.stdout or "delete-xxx" in result.stdout
    assert "-N" in result.stdout or "delete-nnn" in result.stdout
    assert "-Z" in result.stdout or "delete-excl" in result.stdout
    assert "-A" in result.stdout or "heuristic-a" in result.stdout
    assert "-B" in result.stdout or "heuristic-b" in result.stdout


# ── Unit: heuristic -A (largest + newest) ────────────────────────────────────

def test_heuristic_a_keeps_largest_and_newest(tmp_path: Path):
    now = time.time()
    cli   = make_file(tmp_path / "cli.txt",   "a" * 10,  mtime=now - 200)
    large = make_file(tmp_path / "large.txt", "b" * 300, mtime=now - 100)
    new_  = make_file(tmp_path / "new.txt",   "c" * 50,  mtime=now)

    groups = build_near_dupe_groups(
        cli_files={cli}, locate_map={cli: [large, new_]},
        fdupes_groups=[], preferred=[], heuristic_a=True,
    )
    by_path = {e.path: e.action for e in groups[0]}
    assert by_path[large] == "keep"    # overall largest
    assert by_path[new_]  == "keep"    # overall newest
    assert by_path[cli]   == "DELETE"  # neither largest nor newest


def test_heuristic_a_preferred_covers_largest_no_extra_keeper(tmp_path: Path):
    """If preferred file is the overall largest, no extra non-preferred copy kept for size."""
    now = time.time()
    pref_dir = tmp_path / "pref"
    pref_dir.mkdir()
    pref_big  = make_file(pref_dir / "big.txt",   "x" * 500, mtime=now - 200)
    non_small = make_file(tmp_path  / "small.txt", "x" * 10,  mtime=now - 100)
    non_new   = make_file(tmp_path  / "new.txt",   "x" * 50,  mtime=now)

    groups = build_near_dupe_groups(
        cli_files={pref_big}, locate_map={pref_big: [non_small, non_new]},
        fdupes_groups=[], preferred=[str(pref_dir)], heuristic_a=True,
    )
    by_path = {e.path: e for e in groups[0]}
    assert by_path[pref_big].action  == "keep"    # preferred always kept
    assert by_path[non_new].action   == "keep"    # newest non-preferred
    assert by_path[non_small].action == "DELETE"  # not largest, not newest


def test_heuristic_a_comments(tmp_path: Path):
    now = time.time()
    cli   = make_file(tmp_path / "cli.txt",   "a" * 10,  mtime=now - 200)
    large = make_file(tmp_path / "large.txt", "b" * 300, mtime=now - 100)
    new_  = make_file(tmp_path / "new.txt",   "c" * 50,  mtime=now)

    groups = build_near_dupe_groups(
        cli_files={cli}, locate_map={cli: [large, new_]},
        fdupes_groups=[], preferred=[], heuristic_a=True,
    )
    by_path = {e.path: e.comment for e in groups[0]}
    assert "largest in basename group" in by_path[large]
    assert "newest in basename group"  in by_path[new_]
    assert by_path[cli] == ""  # deleted, no keep reason


def test_heuristic_a_winner_gets_both_tags(tmp_path: Path):
    """When one file is both largest and newest it gets both tags."""
    now    = time.time()
    cli    = make_file(tmp_path / "cli.txt",    "x" * 10,  mtime=now - 200)
    winner = make_file(tmp_path / "winner.txt", "x" * 500, mtime=now)

    groups = build_near_dupe_groups(
        cli_files={cli}, locate_map={cli: [winner]},
        fdupes_groups=[], preferred=[], heuristic_a=True,
    )
    winner_comment = next(e.comment for e in groups[0] if e.path == winner)
    assert "largest in basename group" in winner_comment
    assert "newest in basename group"  in winner_comment


# ── Unit: heuristic -B (shallowest path) ─────────────────────────────────────

def test_heuristic_b_keeps_shallowest(tmp_path: Path):
    # Two depth levels: top-level vs nested
    (tmp_path / "sub").mkdir()
    shallow = make_file(tmp_path / "file.txt", "a")
    deep    = make_file(tmp_path / "sub" / "file2.txt", "b")

    groups = build_near_dupe_groups(
        cli_files={shallow}, locate_map={shallow: [deep]},
        fdupes_groups=[], preferred=[], heuristic_b=True,
    )
    by_path = {e.path: e.action for e in groups[0]}
    assert by_path[shallow] == "keep"    # shallowest
    assert by_path[deep]    == "DELETE"


def test_heuristic_b_tie_both_kept(tmp_path: Path):
    """Two files at the same depth both survive."""
    f1 = make_file(tmp_path / "a.txt", "hello")
    f2 = make_file(tmp_path / "b.txt", "world")  # same depth as f1

    groups = build_near_dupe_groups(
        cli_files={f1}, locate_map={f1: [f2]},
        fdupes_groups=[], preferred=[], heuristic_b=True,
    )
    by_path = {e.path: e.action for e in groups[0]}
    assert by_path[f1] == "keep"
    assert by_path[f2] == "keep"


def test_heuristic_b_comment(tmp_path: Path):
    f1 = make_file(tmp_path / "a.txt", "hello")
    sub = tmp_path / "sub"
    sub.mkdir()
    f2 = make_file(sub / "b.txt", "world")

    groups = build_near_dupe_groups(
        cli_files={f1}, locate_map={f1: [f2]},
        fdupes_groups=[], preferred=[], heuristic_b=True,
    )
    by_path = {e.path: e for e in groups[0]}
    assert "shallowest path in group" in by_path[f1].comment
    assert by_path[f2].action == "DELETE"


# ── Unit: heuristics combined ─────────────────────────────────────────────────

def test_heuristic_ab_union_keeps_both(tmp_path: Path):
    """Files elected by either heuristic survive; others are deleted."""
    now = time.time()
    # cli is the reference; large is largest; shallow is at same depth as cli
    (tmp_path / "sub").mkdir()
    cli   = make_file(tmp_path / "cli.txt",          "a" * 10,  mtime=now - 300)
    large = make_file(tmp_path / "sub" / "large.txt","b" * 500, mtime=now - 100)
    new_  = make_file(tmp_path / "sub" / "new.txt",  "c" * 50,  mtime=now)

    groups = build_near_dupe_groups(
        cli_files={cli}, locate_map={cli: [large, new_]},
        fdupes_groups=[], preferred=[], heuristic_a=True, heuristic_b=True,
    )
    by_path = {e.path: e.action for e in groups[0]}
    # cli: shallowest path (heuristic_b) → keep
    # large: heuristic_a (largest) → keep
    # new_: heuristic_a (newest) → keep
    assert by_path[cli]   == "keep"
    assert by_path[large] == "keep"
    assert by_path[new_]  == "keep"


# ── E2E: -p / --prefer runtime flag ──────────────────────────────────────────

def test_e2e_prefer_flag_protects_file(tmp_path: Path, cfg: Path):
    """Files inside a -p DIR are never trashed even without config setting."""
    pref_dir = tmp_path / "important"
    pref_dir.mkdir()
    content = "precious"
    pref_file = make_file(pref_dir / "file.txt", content)
    other     = make_file(tmp_path / "copy.txt", content)

    tsv_path = tmp_path / "out.tsv"
    run_tdupes(
        "--batch", "--tsv", str(tsv_path),
        "--prefer", str(pref_dir),
        str(tmp_path),
        config=cfg,
    )

    assert pref_file.exists(), "file in --prefer DIR must not be trashed"
    rows = read_tsv(tsv_path)
    by_path = {p: a for a, p in rows}
    assert by_path.get(str(pref_file)) == "keep"
    assert by_path.get(str(other)) == "DELETE"


def test_e2e_prefer_flag_additive_with_config(tmp_path: Path, cfg: Path):
    """--prefer adds on top of preferred_directories in config."""
    pref_dir_cfg     = tmp_path / "cfg_pref"
    pref_dir_runtime = tmp_path / "rt_pref"
    pref_dir_cfg.mkdir()
    pref_dir_runtime.mkdir()

    content = "same"
    cfg_file = make_file(pref_dir_cfg     / "f.txt", content)
    rt_file  = make_file(pref_dir_runtime / "f2.txt", content)
    other    = make_file(tmp_path         / "other.txt", content)

    cfg.write_text(
        yaml.dump({**DEFAULT_CONFIG, "preferred_directories": [str(pref_dir_cfg)]})
    )
    tsv_path = tmp_path / "out.tsv"
    run_tdupes(
        "--batch", "--tsv", str(tsv_path),
        "--prefer", str(pref_dir_runtime),
        str(tmp_path),
        config=cfg,
    )

    rows = read_tsv(tsv_path)
    by_path = {p: a for a, p in rows}
    # Both preferred files should be kept
    assert by_path.get(str(cfg_file)) == "keep"
    assert by_path.get(str(rt_file))  == "keep"


def test_e2e_prefer_flag_in_help(tmp_path: Path, cfg: Path):
    result = run_tdupes("--help", config=cfg)
    assert "--prefer" in result.stdout or "-p" in result.stdout


# ── E2E: -x / --exclude runtime flag ─────────────────────────────────────────

def test_e2e_exclude_flag_omits_matching_files(tmp_path: Path, cfg: Path):
    """Files matching a -x pattern are excluded from the results."""
    content = "same content"
    make_file(tmp_path / "a.txt",  content)
    make_file(tmp_path / "b.txt",  content)
    make_file(tmp_path / "c.bak",  content)

    tsv_path = tmp_path / "out.tsv"
    run_tdupes(
        "--batch", "--tsv", str(tsv_path),
        "--exclude", "*.bak",
        str(tmp_path),
        config=cfg,
    )

    assert tsv_path.exists()
    rows = read_tsv(tsv_path)
    paths = [p for _, p in rows]
    assert not any(p.endswith(".bak") for p in paths)


def test_e2e_exclude_flag_additive_with_config(tmp_path: Path, cfg: Path):
    """--exclude patterns stack on top of exclusion_patterns in config."""
    content = "same"
    make_file(tmp_path / "a.txt", content)
    make_file(tmp_path / "b.txt", content)
    make_file(tmp_path / "c.bak", content)
    make_file(tmp_path / "d.tmp", content)

    cfg.write_text(
        yaml.dump({**DEFAULT_CONFIG, "exclusion_patterns": ["*.bak"]})
    )
    tsv_path = tmp_path / "out.tsv"
    run_tdupes(
        "--batch", "--tsv", str(tsv_path),
        "--exclude", "*.tmp",
        str(tmp_path),
        config=cfg,
    )

    rows = read_tsv(tsv_path)
    paths = [p for _, p in rows]
    assert not any(p.endswith(".bak") for p in paths)
    assert not any(p.endswith(".tmp") for p in paths)


def test_e2e_exclude_flag_in_help(tmp_path: Path, cfg: Path):
    result = run_tdupes("--help", config=cfg)
    assert "--exclude" in result.stdout or "-x" in result.stdout
