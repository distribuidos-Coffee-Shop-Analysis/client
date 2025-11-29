#!/usr/bin/env python3
"""
Script to compare two directories of CSV result files.
Assumes files have already been sorted.
Q4 uses intelligent validation instead of line-by-line comparison.
"""

import sys
import os
import csv
import subprocess
from pathlib import Path
from difflib import unified_diff


def validate_q4_file(file_path):
    """
    Validate Q4 results using intelligent validation.
    Returns (is_valid, message)
    """
    script_dir = Path(__file__).parent
    validator_script = script_dir / "validate_q4.py"

    try:
        result = subprocess.run(
            ["python3", str(validator_script), str(file_path)],
            capture_output=True,
            text=True,
        )

        # The validator returns 0 if valid, 1 if invalid
        is_valid = result.returncode == 0
        message = result.stdout if result.stdout else result.stderr

        return is_valid, message
    except Exception as e:
        return False, f"Error running Q4 validator: {e}"


def compare_csv_files(file1, file2):
    """
    Compare two CSV files line by line.
    Returns (is_identical, differences_list)
    """
    with open(file1, "r", encoding="utf-8") as f1:
        lines1 = f1.readlines()

    with open(file2, "r", encoding="utf-8") as f2:
        lines2 = f2.readlines()

    if lines1 == lines2:
        return True, []

    # Generate unified diff
    diff = list(
        unified_diff(
            lines1, lines2, fromfile=str(file1), tofile=str(file2), lineterm=""
        )
    )

    return False, diff


def compare_directories(dir1, dir2, show_diff=False):
    """
    Compare all CSV files between two directories.
    Returns True if all files are identical.
    """
    path1 = Path(dir1)
    path2 = Path(dir2)

    # Files to compare
    files_to_compare = [
        "Q1_results.csv",
        "Q2_best_selling.csv",
        "Q2_most_profits.csv",
        "Q3_results.csv",
        "Q4_results.csv",
    ]

    all_identical = True
    results = {}

    print(f"\nComparando resultados:")
    print(f"  Directorio 1: {dir1}")
    print(f"  Directorio 2: {dir2}")
    print("-" * 80)

    for filename in files_to_compare:
        file1 = path1 / filename
        file2 = path2 / filename

        # Check if both files exist
        if not file1.exists():
            print(f"❌ {filename}: Missing in {dir1}")
            all_identical = False
            results[filename] = "missing_in_dir1"
            continue

        if not file2.exists():
            print(f"❌ {filename}: Missing in {dir2}")
            all_identical = False
            results[filename] = "missing_in_dir2"
            continue

        # Q4 uses intelligent validation instead of line-by-line comparison
        if filename == "Q4_results.csv":
            is_valid, validation_msg = validate_q4_file(file2)

            if is_valid:
                print(f"✅ {filename}: Top 3 válido (formato correcto)")
                results[filename] = "valid_top3"
            else:
                print(f"❌ {filename}: Top 3 inválido")
                all_identical = False
                results[filename] = "invalid_top3"

                if show_diff:
                    print(f"\n   Detalles de validación Q4:")
                    print(validation_msg)
        else:
            # Compare other files normally
            is_identical, diff = compare_csv_files(file1, file2)

            if is_identical:
                print(f"✅ {filename}: Identicos")
                results[filename] = "identical"
            else:
                print(f"❌ {filename}: Diferentes")
                all_identical = False
                results[filename] = "different"

                if show_diff:
                    print(f"\n   Diferencias en {filename}:")
                    # Show first 20 lines of diff
                    for i, line in enumerate(diff[:20]):
                        print(f"   {line}")
                    if len(diff) > 20:
                        print(f"   ... ({len(diff) - 20} more lines)")
                    print()

    print("-" * 80)
    if all_identical:
        print("✅ Todos los archivos bien")
        return True
    else:
        print("❌ Algunos archivos son diferentes")
        return False


def main():
    if len(sys.argv) < 3:
        print(
            "Usage: python compare_results.py <directory1> <directory2> [--show-diff]"
        )
        print("\nExamples:")
        print("  python compare_results.py ./answers ./output/client_1")
        print("  python compare_results.py ./answers ./output/client_2 --show-diff")
        sys.exit(1)

    dir1 = sys.argv[1]
    dir2 = sys.argv[2]
    show_diff = "--show-diff" in sys.argv or "-d" in sys.argv

    if not os.path.isdir(dir1):
        print(f"Error: Directory '{dir1}' does not exist")
        sys.exit(1)

    if not os.path.isdir(dir2):
        print(f"Error: Directory '{dir2}' does not exist")
        sys.exit(1)

    identical = compare_directories(dir1, dir2, show_diff)

    # Exit with appropriate code
    sys.exit(0 if identical else 1)


if __name__ == "__main__":
    main()
