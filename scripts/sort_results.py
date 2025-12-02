#!/usr/bin/env python3
"""
Script to sort CSV result files for comparison.
Each query result has different sorting criteria.
"""

import sys
import csv
import os
from pathlib import Path


def sort_csv_file(input_file, output_file, sort_keys):
    """
    Sort a CSV file by specified columns.

    Args:
        input_file: Path to input CSV file
        output_file: Path to output sorted CSV file
        sort_keys: List of (column_name, is_numeric) tuples for sorting
    """
    with open(input_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        rows = list(reader)

    if not rows:
        # Empty file, just copy header
        with open(output_file, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
        return

    # Sort rows by specified keys
    def sort_key(row):
        key = []
        for col_name, is_numeric in sort_keys:
            value = row.get(col_name, "")
            if is_numeric:
                try:
                    key.append(float(value) if value else 0.0)
                except ValueError:
                    key.append(0.0)
            else:
                key.append(value)
        return tuple(key)

    sorted_rows = sorted(rows, key=sort_key)

    # Write sorted data
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(sorted_rows)


def sort_q1_results(input_file, output_file):
    """Sort Q1 results by transaction_id"""
    sort_csv_file(input_file, output_file, [("transaction_id", False)])


def sort_q2_results(input_file, output_file):
    """Sort Q2 results by year_month and item_name"""
    sort_csv_file(
        input_file, output_file, [("year_month", False), ("item_name", False)]
    )


def sort_q3_results(input_file, output_file):
    """Sort Q3 results by year_half and store_name"""
    sort_csv_file(
        input_file, output_file, [("year_half", False), ("store_name", False)]
    )


def sort_q4_results(input_file, output_file):
    """Sort Q4 results by store_name and birthdate"""
    sort_csv_file(
        input_file, output_file, [("store_name", False), ("birthdate", False)]
    )


def sort_directory(input_dir, output_dir=None):
    """
    Sort all CSV files in a directory.
    If output_dir is None, files are sorted in-place.
    """
    input_path = Path(input_dir)

    if output_dir:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
    else:
        output_path = input_path

    # Define sorting functions for each file
    sort_functions = {
        "Q1_results.csv": sort_q1_results,
        "Q2_best_selling.csv": sort_q2_results,
        "Q2_most_profits.csv": sort_q2_results,
        "Q3_results.csv": sort_q3_results,
        "Q4_results.csv": sort_q4_results,
    }

    sorted_count = 0
    for filename, sort_func in sort_functions.items():
        input_file = input_path / filename
        output_file = output_path / filename

        if input_file.exists():
            print(f"Ordenando {filename}...")
            sort_func(str(input_file), str(output_file))
            sorted_count += 1
        else:
            print(f"Advertencia: {filename} no encontrado en {input_dir}")

    print(f"\nOrdenados {sorted_count} archivos en {input_dir}")
    return sorted_count


def main():
    if len(sys.argv) < 2:
        print("Usage: python sort_results.py <directory> [output_directory]")
        print("\nExamples:")
        print("  python sort_results.py ./answers              # Sort in-place")
        print("  python sort_results.py ./output/client_1      # Sort in-place")
        print(
            "  python sort_results.py ./output/client_1 ./sorted_output  # Sort to new directory"
        )
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else None

    if not os.path.isdir(input_dir):
        print(f"Error: Directory '{input_dir}' does not exist")
        sys.exit(1)

    sort_directory(input_dir, output_dir)


if __name__ == "__main__":
    main()
