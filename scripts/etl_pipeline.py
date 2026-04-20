from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(r"[^a-z0-9]+", "_", regex=True)
        .str.strip("_")
    )
    result = df.copy()
    result.columns = cleaned
    return result


def basic_clean(df: pd.DataFrame) -> pd.DataFrame:
    result = normalize_columns(df)
    result = result.drop_duplicates().reset_index(drop=True)
    for column in result.select_dtypes(include="object").columns:
        result[column] = result[column].astype("string").str.strip()
    return result


def build_clean_dataset(input_path: Path) -> pd.DataFrame:
    df = pd.read_csv(input_path)
    return basic_clean(df)


def save_processed(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Olist ETL pipeline.")
    parser.add_argument("--input",  required=True, type=Path, help="Path to raw CSV in data/raw/.")
    parser.add_argument("--output", required=True, type=Path, help="Path to output CSV in data/processed/.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cleaned_df = build_clean_dataset(args.input)
    save_processed(cleaned_df, args.output)
    print(f"Saved: {args.output}  |  Rows: {len(cleaned_df)}  |  Columns: {len(cleaned_df.columns)}")


if __name__ == "__main__":
    main()
