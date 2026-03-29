# tdupes

Find and manage duplicate files on Linux.

`tdupes` uses `fdupes` to locate exact duplicates, produces a reviewable TSV,
then trashes confirmed deletions via `gio trash` so nothing is irrecoverably lost.

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
  PATH               Files or directories to scan for duplicates

Options:
  -l, --locate       Expand file arguments via locatedb (exact basename matches)
  -L, --locate-all   Like -l, but also tabulate near-duplicates (same basename,
                     not byte-identical) with real similarity codes
  -t FILE, --tsv FILE
                     Path for the output TSV (default: temp file)
  -p DIR, --prefer DIR
                     Mark DIR as preferred at runtime (files inside are never
                     proposed for deletion). Additive with config. Repeatable.
  -x PATTERN, --exclude PATTERN
                     Shell glob to exclude files by full path. Additive with
                     config. Repeatable: -x '*.tmp' -x '/mnt/*'
  -b, --batch        Batch mode: no prompts; execute DELETE actions immediately
  -v, --verbose      Increase output verbosity
  -q, --quiet        Reduce output verbosity
  -c, --config FILE  Config file path (default: $XDG_CONFIG_HOME/tdupes.yml)
  -V, --version      Show version and exit
  -h, --help         Show this help message and exit
```

### Examples

```bash
# Scan two directories interactively
tdupes ~/Pictures ~/Downloads

# Use locate to also find exact-duplicate copies of a specific file
tdupes --locate ~/Downloads/photo.jpg ~/Pictures

# Use locate and also include near-duplicates (same basename, different content)
tdupes -L ~/Downloads/photo.jpg ~/Pictures

# Batch mode (good for scripting / cron)
tdupes --batch ~/Documents

# Write the TSV to a specific path
tdupes -t /tmp/dupes.tsv ~/Music ~/Videos
```

## Config

On first run `tdupes` creates `$XDG_CONFIG_HOME/tdupes.yml` (defaults to
`~/.config/tdupes.yml`):

```yaml
preferred_directories: []   # files here are never marked DELETE
verbosity: 1                # 0=quiet, 1=normal, 2=verbose
tsv_output: null            # null = temp file each run
exclusion_patterns: []      # shell glob patterns to skip
batch_mode: false
```

**`preferred_directories`** — any file whose path begins with one of these
directories will be marked `keep` regardless of group ordering.

## TSV format

```
Action  Similarity  Size_KB  Modified              Path                              Comment
keep    100         2048.0   2024-11-01T14:22:10   /home/user/Pictures/photo.jpg     in preferred folder
DELETE  100         2048.0   2024-09-15T08:01:55   /home/user/Downloads/photo.jpg
```

| Column     | Values                                                                        |
|------------|-------------------------------------------------------------------------------|
| Action     | `keep` or `DELETE` — edit freely before confirming                            |
| Similarity | `100` exact · `XXX` binary same size · `NNN` text % match · `!!!` binary diff size |
| Size_KB    | File size in kilobytes                                                        |
| Modified   | Last-modified timestamp (ISO 8601)                                            |
| Path       | Absolute file path                                                            |
| Comment    | Reason for the proposed action (see below) — informational, ignored on re-read |

Groups are separated by blank lines. The first entry in each group is either
the file given as a CLI argument, or the newest copy.

Near-duplicate groups (found with `-L`) are written in a separate section after
the exact-duplicate groups, preceded by a `#` comment line.

### Default Action logic

**Exact-duplicate groups** (byte-identical per fdupes):

| Comment tag           | Rule                                                         |
|-----------------------|--------------------------------------------------------------|
| `in preferred folder` | File is inside a `preferred_directories` path → **keep**    |
| `last in group`       | Last file in the group (tiebreaker) → **keep**              |
| *(no tag)*            | All other copies → **DELETE**                               |

> **CLI argument files are listed first** in each group so they are *never* the
> last-in-group tiebreaker and therefore receive **DELETE** by default
> (unless they also fall under a preferred folder rule).

**Near-duplicate groups** (`-L`, same basename, not byte-identical):

| Comment tag                   | Rule                                                                  |
|-------------------------------|-----------------------------------------------------------------------|
| `in preferred folder`         | File is inside a `preferred_directories` path → **keep**             |
| `largest in basename group`   | Overall largest file in the group, *only if* no preferred file is larger → **keep** |
| `newest in basename group`    | Overall newest file in the group, *only if* no preferred file is newer → **keep** |
| *(no tag)*                    | Everything else → **DELETE**                                          |

> **CLI argument files are listed first** and may receive **DELETE** if they are
> neither the largest nor the newest (and not in a preferred folder).
>
> If a preferred-folder file is already the overall largest (or newest), no extra
> non-preferred copy is kept for that reason — the preferred file already covers it.

Multiple tags are comma-separated (e.g. `largest in basename group, newest in basename group`).
The Comment column is read-only — it is ignored when tdupes re-reads the TSV after you edit it.

## Interactive flow

1. `tdupes` scans paths and prints the duplicate table.
2. The TSV is opened with `xdg-open` for manual review.
3. You edit `Action` cells (change `DELETE` → `keep` or vice-versa), save, return.
4. `tdupes` re-reads the TSV and asks for confirmation.
5. On confirmation, all `DELETE` files are sent to the trash via `gio trash`.
6. A summary shows how many files were trashed and how much space was freed.

Files trashed with `gio trash` remain recoverable from the system trash until
the bin is emptied.

## License

MIT
