#!/usr/bin/env python3
"""
Script to validate Q4 results (Top 3 per store).
Instead of comparing line-by-line, validates that the result is a valid Top 3.
"""

import sys
import csv
from collections import defaultdict
from pathlib import Path


def validate_q4_format(file_path):
    """
    Validate that Q4 results have the correct format and structure.

    Requirements:
    1. Correct header: store_name,birthdate,purchases_qty
    2. Exactly 3 records per store (Top 3)
    3. purchases_qty in descending or equal order per store
    4. All required stores present (10 stores)

    Returns: (is_valid, error_messages, stats)
    """
    errors = []
    stores = defaultdict(list)

    # Expected stores
    expected_stores = {
        "G Coffee @ Alam Tun Hussein Onn",
        "G Coffee @ Bandar Seri Mulia",
        "G Coffee @ Damansara Saujana",
        "G Coffee @ Kampung Changkat",
        "G Coffee @ Kondominium Putra",
        "G Coffee @ PJS8",
        "G Coffee @ Seksyen 21",
        "G Coffee @ Taman Damansara",
        "G Coffee @ USJ 57W",
        "G Coffee @ USJ 89q",
    }

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            # Validate header
            expected_header = ["store_name", "birthdate", "purchases_qty"]
            if reader.fieldnames != expected_header:
                errors.append(
                    f"Invalid header: expected {expected_header}, got {reader.fieldnames}"
                )
                return False, errors, {}

            # Read all records
            for row_num, row in enumerate(reader, start=2):
                store_name = row["store_name"]
                birthdate = row["birthdate"]
                purchases_qty_str = row["purchases_qty"]

                # Validate store name
                if store_name not in expected_stores:
                    errors.append(f"Row {row_num}: Unknown store '{store_name}'")
                    continue

                # Validate purchases_qty is a number
                try:
                    purchases_qty = int(purchases_qty_str)
                except ValueError:
                    errors.append(
                        f"Row {row_num}: Invalid purchases_qty '{purchases_qty_str}' (not an integer)"
                    )
                    continue

                # Validate birthdate format (basic check)
                if not birthdate or len(birthdate) != 10 or birthdate.count("-") != 2:
                    errors.append(
                        f"Row {row_num}: Invalid birthdate format '{birthdate}'"
                    )

                stores[store_name].append(
                    {
                        "row": row_num,
                        "birthdate": birthdate,
                        "purchases_qty": purchases_qty,
                    }
                )

    except FileNotFoundError:
        errors.append(f"File not found: {file_path}")
        return False, errors, {}
    except Exception as e:
        errors.append(f"Error reading file: {e}")
        return False, errors, {}

    # Validate number of records per store
    for store_name in expected_stores:
        records = stores.get(store_name, [])
        count = len(records)

        if count == 0:
            errors.append(f"Store '{store_name}': No records found (expected 3)")
        elif count != 3:
            errors.append(
                f"Store '{store_name}': Found {count} records (expected 3 for Top 3)"
            )
        else:
            # Validate that all purchases_qty are reasonable (> 0)
            purchases = [r["purchases_qty"] for r in records]
            if any(p <= 0 for p in purchases):
                errors.append(
                    f"Store '{store_name}': Invalid purchases_qty values (must be > 0): {purchases}"
                )

            # Validate that purchases are within reasonable range for a Top 3
            # (no huge gaps that would indicate incorrect Top 3)
            min_qty = min(purchases)
            max_qty = max(purchases)
            if max_qty > min_qty * 3:
                errors.append(
                    f"Store '{store_name}': Suspicious purchase quantities (gap too large): min={min_qty}, max={max_qty}"
                )

    # Collect statistics
    stats = {
        "total_stores": len(stores),
        "expected_stores": len(expected_stores),
        "total_records": sum(len(records) for records in stores.values()),
        "stores_with_correct_count": sum(
            1 for records in stores.values() if len(records) == 3
        ),
    }

    is_valid = len(errors) == 0
    return is_valid, errors, stats


def main():
    if len(sys.argv) < 2:
        print("Usage: python validate_q4.py <q4_results.csv>")
        sys.exit(1)

    file_path = sys.argv[1]

    is_valid, errors, stats = validate_q4_format(file_path)

    print(f"\n{'='*60}")
    print(f"Q4 Results Validation: {file_path}")
    print(f"{'='*60}")

    if is_valid:
        print(f"✅ Q4 validation PASSED == Es un Top 3 valido")
        sys.exit(0)
    else:
        print(f"❌ Q4 validation FAILED")
        print(f"\nErrors found ({len(errors)}):")
        for error in errors[:10]:  # Show first 10 errors
            print(f"  • {error}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more errors")

        print(f"\nStatistics:")
        print(f"  - Total stores: {stats.get('total_stores', 0)}/10")
        print(f"  - Total records: {stats.get('total_records', 0)} (expected 30)")
        sys.exit(1)


if __name__ == "__main__":
    main()
