# tdupes

Find, review, and safely trash duplicate files on Linux.

`tdupes` detects **exact duplicates** (byte-identical, via `fdupes`) and
optionally **basename matches** (same filename, scored by content similarity,
via `plocate`/`locate`).  Results are written to a TSV that you review and
edit with your favourite spreadsheet tool before any files are touched;
confirmed deletions go to `gio trash` and remain recoverable until the bin
is emptied.

Key features:

* Accepts any mix of individual files and directories as arguments
* Basename-match detection with `-L`: finds same-named files across the
  filesystem, scored by similarity (`100` exact · `XXX` binary same-size ·
  `NNN%` text match · `!!!` binary different-size)
* Preferred-directory protection — files inside protected dirs are never
  proposed for deletion; on first run, all system dirs at `/` (except
  `/home` and `/tmp`) are pre-configured as preferred by default
* Runtime preferred-dir control: `-p` add, `-r` remove, `-s`/`-S` toggle
  the full set of system dirs for a single run
* Exclusion patterns via config or `-x` at runtime
* Interactive TSV review (opened with `xdg-open`) or fully automated batch mode

## Install

```bash
pip install tdupes
```

System dependencies (Ubuntu/Debian):

```bash
sudo apt install fdupes plocate gvfs-bin xdg-utils
```

## Usage

```
tdupes [OPTIONS] PATH [PATH ...]

Positional arguments:
  PATH                   Files or directories to scan for duplicates

Options:
  -l, --locate           Search locatedb for basename copies of each file arg
                         and add them to the scan (exact duplicates only)
  -L, --locate-all       Like -l, but also tabulate basename matches that are
                         not byte-identical, with similarity codes
  -X, --delete-xxx       (-L) DELETE non-preferred XXX matches
                         (binary, equal size, not identical)
  -N, --delete-nnn       (-L) DELETE non-preferred NNN matches
                         (text files, partial % similarity)
  -Z, --delete-excl      (-L) DELETE non-preferred !!! matches
                         (binary, different size)
  -A, --heuristic-a      (-L) Keep the largest and newest non-preferred file
                         in each basename group; delete the rest
  -B, --heuristic-b      (-L) Keep the shallowest-path non-preferred file(s)
                         in each basename group; delete the rest
  -t FILE, --tsv FILE    Path for the output TSV (default: temp file)
  -p DIR, --prefer DIR   Add DIR to preferred directories for this run.
                         Additive with config. Repeatable.
  -r DIR, --remove-prefer DIR
                         Remove DIR from preferred directories for this run,
                         overriding config and -p. Repeatable.
  -s, --system-prefer    Add all top-level system dirs at / (except /home and
                         /tmp) to preferred dirs for this run
  -S, --no-system-prefer Remove the system dirs from preferred dirs for this run
  -x PATTERN, --exclude PATTERN
                         Shell glob to exclude files by full path. Additive
                         with config. Repeatable: -x '*.tmp' -x '/mnt/*'
  -b, --batch            Batch mode: no prompts; execute DELETE actions immediately
  -v, --verbose          Increase output verbosity
  -q, --quiet            Reduce output verbosity
  -c FILE, --config FILE Config file path (default: $XDG_CONFIG_HOME/tdupes.yml)
  -V, --version          Show version and exit
  -h, --help             Show this help message and exit
```

### Examples

```bash
# Scan two directories interactively
tdupes ~/Pictures ~/Downloads

# Find exact-duplicate copies of a specific file via locatedb
tdupes -l ~/Downloads/photo.jpg ~/Pictures

# Find basename matches too; keep all by default (review in TSV)
tdupes -L ~/Downloads/photo.jpg ~/Pictures

# Delete binary matches automatically in addition to finding exact dupes
tdupes -L -X -Z ~/Downloads/photo.jpg ~/Pictures

# Keep only largest+newest per basename group; delete the rest
tdupes -L -A ~/Downloads/photo.jpg ~/Pictures

# Scan freely, ignoring preferred-directory protection from config
tdupes -S ~/Documents

# Batch mode (good for scripting / cron)
tdupes --batch ~/Documents

# Write the TSV to a specific path
tdupes -t /tmp/dupes.tsv ~/Music ~/Videos
```

## Config

On first run `tdupes` creates `$XDG_CONFIG_HOME/tdupes.yml` (defaults to
`~/.config/tdupes.yml`) and pre-populates `preferred_directories` with all
top-level system directories at `/` (excluding `/home` and `/tmp`), so that
system files are protected immediately without any manual configuration:

```yaml
preferred_directories:    # pre-filled with system dirs on first run
  - /bin
  - /boot
  - /etc
  - /usr
  # … etc.
verbosity: 1              # 0=quiet, 1=normal, 2=verbose
tsv_output: null          # null = temp file each run
exclusion_patterns: []    # shell glob patterns to skip
batch_mode: false
```

**`preferred_directories`** — any file whose path begins with one of these
directories is marked `keep` regardless of group ordering, and cannot be
overridden by `-X`/`-N`/`-Z` flags.

Use `-r DIR` to temporarily remove a directory from the protected set for one
run; use `-S` to remove all system dirs at once; use `-s` to add them back if
you have removed them from config.

## TSV format

```
Action  Similarity  Size_KB  Modified              Path                              Comment
keep    100         2048.0   2024-11-01T14:22:10   /home/user/Pictures/photo.jpg     in preferred folder
DELETE  100         2048.0   2024-09-15T08:01:55   /home/user/Downloads/photo.jpg
```

| Column     | Values                                                                              |
|------------|-------------------------------------------------------------------------------------|
| Action     | `keep` or `DELETE` — edit freely before confirming                                  |
| Similarity | `100` exact · `XXX` binary same size · `NNN` text % match · `!!!` binary diff size |
| Size_KB    | File size in kilobytes                                                              |
| Modified   | Last-modified timestamp (ISO 8601)                                                  |
| Path       | Absolute file path                                                                  |
| Comment    | Reason for the keep decision — informational, ignored on re-read                    |

Groups are separated by blank lines.  The CLI argument file is always listed
first in each group.  Basename match groups (found with `-L`) are written in a
separate section after the exact-duplicate groups, preceded by a `#` comment line.

### Default Action logic

**Exact-duplicate groups** (byte-identical per fdupes):

| Comment tag           | Rule                                                         |
|-----------------------|--------------------------------------------------------------|
| `in preferred folder` | File is inside a `preferred_directories` path → **keep**    |
| `last in group`       | Last file in the group (tiebreaker) → **keep**              |
| *(no tag)*            | All other copies → **DELETE**                               |

> **CLI argument files are listed first** in each group so they are *never* the
> last-in-group tiebreaker and therefore receive **DELETE** by default
> (unless they fall under a preferred folder rule).

**Basename match groups** (`-L`, same basename, not byte-identical):

| Comment tag           | Rule                                                                          |
|-----------------------|-------------------------------------------------------------------------------|
| `in preferred folder` | File is inside a `preferred_directories` path → **keep** (always)            |
| *(no tag)*            | All other files → **keep** by default                                         |

Use `-X`, `-N`, `-Z` to DELETE non-preferred files by similarity type:

| Flag | Similarity | Meaning                              |
|------|------------|--------------------------------------|
| `-X` | `XXX`      | Binary files of equal size           |
| `-N` | `NNN`      | Text files with partial % similarity |
| `-Z` | `!!!`      | Binary files of different size       |

Use `-A` and `-B` to apply heuristics that auto-select which non-preferred files to keep:

| Flag | Heuristic                                                                       |
|------|---------------------------------------------------------------------------------|
| `-A` | Keep the **largest** and the **newest** non-preferred file in the group         |
| `-B` | Keep the **shallowest-path** non-preferred file(s) (ties all kept)              |

Flags `-A` and `-B` can be combined — the union of their keep sets survives; the rest are deleted.
Preferred-directory files are always kept regardless of any flag.

The Comment column is read-only — it is ignored when tdupes re-reads the TSV
after you edit it.

## Interactive flow

1. `tdupes` scans paths and prints the duplicate table.
2. The TSV is opened with `xdg-open` for manual review.
3. You edit `Action` cells (change `DELETE` → `keep` or vice-versa), save, return.
4. `tdupes` re-reads the TSV, displays the updated table, and asks for confirmation.
5. On confirmation, all `DELETE` files are sent to the trash via `gio trash`.
6. A summary shows how many files were trashed and how much space was freed.

Files trashed with `gio trash` remain recoverable from the system trash until
the bin is emptied.

## License

MIT
