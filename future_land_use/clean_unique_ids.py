import argparse
import re
import pandas as pd
from pathlib import Path

SPATIAL_EXTENSIONS = {'.shp', '.gpkg', '.geojson', '.gdb'}
TABLE_EXTENSIONS = {'.csv', '.xlsx', '.xls'}


def clean_id(value):
    """Clean a unique ID: collapse whitespace to single underscore, remove non-alphanumeric except - and _."""
    s = str(value).strip() # convert to string and trim whitespace from ends
    s = re.sub(r'\s*-\s*', '-', s) # collapse spaces around hyphens
    s = re.sub(r'\s+', '_', s) # collapse all whitespace to single underscore
    s = re.sub(r'[^A-Za-z0-9_.-]', '', s) # remove all non-alphanumeric except dashes, underscores, and periods
    return s


def main():
    parser = argparse.ArgumentParser(description="Clean a unique ID field in a spatial layer or table.")
    parser.add_argument("file", help="Path to the input file (.shp, .gpkg, .geojson, .csv, .xlsx)")
    parser.add_argument("id_column", help="Name of the unique ID column to clean")
    args = parser.parse_args()

    file_path = Path(args.file)
    ext = file_path.suffix.lower()

    if ext in SPATIAL_EXTENSIONS:
        import geopandas as gpd
        df = gpd.read_file(args.file)
        is_spatial = True
    elif ext in TABLE_EXTENSIONS:
        if ext in ('.xlsx', '.xls'):
            df = pd.read_excel(args.file)
        else:
            try:
                df = pd.read_csv(args.file)
            except UnicodeDecodeError:
                df = pd.read_csv(args.file, encoding='latin-1')
        is_spatial = False
    else:
        raise ValueError(f"Unsupported file extension '{ext}'. Supported: {SPATIAL_EXTENSIONS | TABLE_EXTENSIONS}")

    if args.id_column not in df.columns:
        raise ValueError(f"Column '{args.id_column}' not found. Available: {list(df.columns)}")

    df[args.id_column] = df[args.id_column].apply(clean_id)

    if is_spatial:
        out_path = str(file_path.with_suffix('')) + '_cleaned.gpkg'
        df.to_file(out_path, driver='GPKG')
    else:
        out_path = str(file_path.with_suffix('')) + '_cleaned.csv'
        df.to_csv(out_path, index=False)

    print(f"Output: {out_path} ({len(df)} rows)")


if __name__ == '__main__':
    main()
