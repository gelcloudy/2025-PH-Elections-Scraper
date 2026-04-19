#!/usr/bin/env python3
import csv
from pathlib import Path

# Input folders
LOCAL_DIR = Path("output/local")
OVERSEAS_DIR = Path("output/overseas")

# Output files (exactly 2)
OUT_LOCAL = Path("output/combined_local.csv")
OUT_OVERSEAS = Path("output/combined_overseas.csv")

# Region order (same logical order used by your scraper)
LOCAL_REGION_ORDER = [
    "BARMM", "CAR", "NCR", "NIR", "LAV",
    "I", "II", "III", "IVA", "IVB",
    "V", "VI", "VII", "VIII", "IX",
    "X", "XI", "XII", "XIII",
]

OVERSEAS_REGION_ORDER = [
    "ASIA_PACIFIC",
    "NORTH_AND_LATIN_AMERICAS",
    "MIDDLE_EAST_AND_AFRICAS",
    "EUROPE",
]


def region_key_from_filename(path: Path) -> str:
    # Expected: region_<KEY>_<CODE>.csv
    # Example: region_NCR_R0NCR00.csv -> NCR
    name = path.stem  # no .csv
    if not name.startswith("region_"):
        return ""
    body = name[len("region_"):]
    parts = body.rsplit("_", 1)
    if len(parts) != 2:
        return ""
    return parts[0]


def merge_group(input_dir: Path, output_file: Path, region_order: list[str]) -> None:
    files = sorted(input_dir.glob("region_*.csv"))
    if not files:
        print(f"[WARN] No CSV files found in {input_dir}")
        return

    order_index = {k: i for i, k in enumerate(region_order)}

    # Sort files by explicit region order; unknown region keys go last (alphabetical)
    files.sort(
        key=lambda p: (
            order_index.get(region_key_from_filename(p), 10**9),
            region_key_from_filename(p),
            p.name,
        )
    )

    header = None
    total_rows = 0
    seen_rows = set()
    dedupe_keys = None

    output_file.parent.mkdir(parents=True, exist_ok=True)

    with output_file.open("w", newline="", encoding="utf-8") as out_f:
        writer = None

        for file_path in files:
            region_key = region_key_from_filename(file_path)
            print(f"[INFO] Reading {file_path.name} (region={region_key})")

            with file_path.open("r", newline="", encoding="utf-8") as in_f:
                reader = csv.DictReader(in_f)

                if reader.fieldnames is None:
                    print(f"[WARN] Skipping empty/invalid CSV: {file_path.name}")
                    continue

                if header is None:
                    header = reader.fieldnames
                    writer = csv.DictWriter(out_f, fieldnames=header, extrasaction="ignore")
                    writer.writeheader()

                    if output_file.name == "combined_overseas.csv":
                        dedupe_keys = [
                            "region",
                            "country",
                            "jurisdiction",
                            "precinct_id",
                            "contest_type",
                            "contest_code",
                            "candidate",
                        ]
                else:
                    # Keep output schema consistent with first file
                    if reader.fieldnames != header:
                        print(f"[WARN] Header mismatch in {file_path.name}; writing only known columns")

                for row in reader:
                    if dedupe_keys:
                        row_key = tuple(row.get(key, "") for key in dedupe_keys)
                        if row_key in seen_rows:
                            continue
                        seen_rows.add(row_key)

                    writer.writerow(row)
                    total_rows += 1

    print(f"[DONE] Wrote {total_rows:,} rows -> {output_file}")


def main() -> None:
    merge_group(LOCAL_DIR, OUT_LOCAL, LOCAL_REGION_ORDER)
    merge_group(OVERSEAS_DIR, OUT_OVERSEAS, OVERSEAS_REGION_ORDER)


if __name__ == "__main__":
    main()