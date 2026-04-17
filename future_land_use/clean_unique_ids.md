# clean_unique_ids.py

Cleans unique ID columns in a geodatabase layer or tabular file by collapsing whitespace to underscores, normalizing hyphens, and stripping non-alphanumeric characters (except `-`, `_`, and `.`).

## Usage

```
uv run python future_land_use/clean_unique_ids.py <file> <id_columns...> [--layer LAYER]
```

| Argument | Required | Description |
| --- | --- | --- |
| `file` | Yes | Path to input file (`.gdb`, `.csv`, `.xlsx`, `.xls`) |
| `id_columns` | Yes | One or more column names to clean (space-separated) |
| `--layer` | Only for `.gdb` | Layer name within the geodatabase |

## Examples

### CSV

```
uv run python future_land_use/clean_unique_ids.py "C:/data/Zoning_2026_d2.csv" Juris_zn
```

Writes `Zoning_2026_d2_cleaned.csv` in the same directory.

### Geodatabase

```
uv run python future_land_use/clean_unique_ids.py "C:/data/FLU_draft2.gdb" Juris_zn --layer FLU2025
```

Writes a new layer `FLU2025_cleaned` into the same `.gdb`.

### Multiple columns

```
uv run python future_land_use/clean_unique_ids.py "C:/data/zones.xlsx" Juris_zn Zone_ID
```

## What the cleaning does

1. Trims leading/trailing whitespace
2. Collapses spaces around hyphens (e.g. `R - 1` → `R-1`)
3. Replaces remaining whitespace with a single underscore (e.g. `Zone  A` → `Zone_A`)
4. Removes all characters except letters, digits, `-`, `_`, and `.`
