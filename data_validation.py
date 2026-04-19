import pandas as pd
from datetime import datetime


def load_dataset(filepath):
    print(f"Loading {filepath}...")
    return pd.read_csv(filepath)


def get_key_fields(dataset_type):
    if dataset_type == "local":
        return ["region", "province", "municipality", "barangay", "precinct_id", "contest_type", "contest_code", "candidate"]
    return ["region", "country", "jurisdiction", "precinct_id", "contest_type", "contest_code", "candidate"]


def build_duplicate_key_report(df, dataset_type):
    report = []
    key_fields = [field for field in get_key_fields(dataset_type) if field in df.columns]

    report.append("\n3. DUPLICATES")
    exact_duplicates = int(df.duplicated().sum())
    report.append(f"   Exact duplicate rows: {exact_duplicates:,}")
    if exact_duplicates > 0:
        report.append(f"   [ERROR] {exact_duplicates / len(df) * 100:.2f}% of data is duplicated")
        report.append(f"   Rows after dropping exact duplicates: {len(df) - exact_duplicates:,}")
    else:
        report.append("   [OK] No exact duplicate rows found")

    if key_fields:
        key_dupes = int(df.duplicated(subset=key_fields).sum())
        report.append(f"   Key-based duplicate rows ({', '.join(key_fields)}): {key_dupes:,}")
        if key_dupes > 0:
            dup_groups = (
                df.groupby(key_fields, dropna=False)
                .size()
                .reset_index(name="count")
                .query("count > 1")
                .sort_values(["count"] + key_fields, ascending=[False] + [True] * len(key_fields))
            )
            report.append("   Top repeated groups:")
            for _, row in dup_groups.head(10).iterrows():
                key_desc = ", ".join(f"{field}={row[field]}" for field in key_fields)
                report.append(f"      {row['count']:,}x  {key_desc}")

            if dataset_type == "overseas" and {"country", "jurisdiction", "precinct_id", "contest_code", "candidate"}.issubset(df.columns):
                duplicate_rows = df[df.duplicated(subset=key_fields, keep=False)]
                report.append("   Overseas duplicate concentration by geography:")
                for field in ["country", "jurisdiction", "precinct_id", "contest_code", "candidate"]:
                    if field in duplicate_rows.columns:
                        grouped = duplicate_rows.groupby(field).size().sort_values(ascending=False).head(5)
                        report.append(f"      Top {field} values among key duplicates:")
                        for value, count in grouped.items():
                            report.append(f"         {value}: {count:,}")
        else:
            report.append("   [OK] No key-based duplicates found")

    return report


def validate_data(df, dataset_type="local"):
    validation = []

    validation.append("\n1. DATASET OVERVIEW")
    validation.append(f"   Total rows: {len(df):,}")
    validation.append(f"   Total columns: {len(df.columns)}")
    validation.append(f"   Columns: {', '.join(df.columns.tolist())}")

    validation.append("\n2. MISSING VALUES")
    missing = df.isnull().sum()
    if missing.sum() == 0:
        validation.append("   [OK] No missing values found")
    else:
        for col, count in missing[missing > 0].items():
            pct = (count / len(df)) * 100
            validation.append(f"   [ERROR] {col}: {count:,} ({pct:.2f}%)")

    validation.extend(build_duplicate_key_report(df, dataset_type))

    validation.append("\n4. NUMERIC FIELD VALIDATION")
    numeric_cols = ["registered_voters", "actual_voters", "valid_ballots", "votes", "turnout_pct", "percentage"]
    for col in numeric_cols:
        if col in df.columns:
            negative_vals = int((df[col] < 0).sum())
            if negative_vals > 0:
                validation.append(f"   [ERROR] {col}: {negative_vals:,} negative values")
            else:
                validation.append(f"   [OK] {col}: All non-negative")

    if "turnout_pct" in df.columns:
        validation.append("\n5. TURNOUT PERCENTAGE VALIDATION")
        invalid_turnout = int(((df["turnout_pct"] < 0) | (df["turnout_pct"] > 100)).sum())
        if invalid_turnout > 0:
            validation.append(f"   [ERROR] Invalid turnout values (< 0 or > 100): {invalid_turnout:,}")
            bad_vals = df[(df["turnout_pct"] < 0) | (df["turnout_pct"] > 100)]["turnout_pct"].unique()
            validation.append(f"   Found values: {sorted(bad_vals)[:10]}")
        else:
            validation.append("   [OK] All turnout percentages valid (0-100%)")
        validation.append(
            f"   Min: {df['turnout_pct'].min():.2f}%, Max: {df['turnout_pct'].max():.2f}%, Avg: {df['turnout_pct'].mean():.2f}%"
        )

    if "percentage" in df.columns:
        validation.append("\n6. CANDIDATE VOTE PERCENTAGE VALIDATION")
        invalid_pct = int(((df["percentage"] < 0) | (df["percentage"] > 100)).sum())
        if invalid_pct > 0:
            validation.append(f"   [ERROR] Invalid percentage values: {invalid_pct:,}")
        else:
            validation.append("   [OK] All percentages valid (0-100%)")

    validation.append("\n7. CATEGORICAL FIELDS")
    if "contest_type" in df.columns:
        contest_types = df["contest_type"].value_counts()
        validation.append("   contest_type distribution:")
        for val, count in contest_types.items():
            validation.append(f"      {val}: {count:,}")

    validation.append("\n8. UNIQUE VALUES COUNT")
    if dataset_type == "local":
        unique_fields = ["region", "province", "municipality", "barangay", "precinct_id", "contest_code", "candidate"]
    else:
        unique_fields = ["region", "country", "jurisdiction", "precinct_id", "contest_code", "candidate"]

    for field in unique_fields:
        if field in df.columns:
            validation.append(f"   {field}: {df[field].nunique():,}")

    validation.append("\n9. GEOGRAPHIC DISTRIBUTION (Top 10)")
    if dataset_type == "local" and "province" in df.columns:
        geo_dist = df["province"].value_counts().head(10)
        validation.append("   Top provinces by row count:")
    elif dataset_type == "overseas" and "country" in df.columns:
        geo_dist = df["country"].value_counts().head(10)
        validation.append("   Top countries by row count:")
    else:
        geo_dist = df["region"].value_counts().head(10)
        validation.append("   Top regions by row count:")

    for geo, count in geo_dist.items():
        validation.append(f"      {geo}: {count:,}")

    validation.append("\n10. DATA TYPES")
    for col in df.columns:
        validation.append(f"   {col}: {df[col].dtype}")

    validation.append("\n11. CONSISTENCY CHECKS")
    if "registered_voters" in df.columns and "actual_voters" in df.columns:
        inconsistent = int((df["actual_voters"] > df["registered_voters"]).sum())
        if inconsistent > 0:
            validation.append(f"   [ERROR] Actual voters > registered voters: {inconsistent:,} rows")
        else:
            validation.append("   [OK] Actual voters <= registered voters")

    if "valid_ballots" in df.columns and "actual_voters" in df.columns:
        inconsistent = int((df["valid_ballots"] > df["actual_voters"]).sum())
        if inconsistent > 0:
            validation.append(f"   [ERROR] Valid ballots > actual voters: {inconsistent:,} rows")
        else:
            validation.append("   [OK] Valid ballots <= actual voters")

    validation.append("\n12. STRUCTURE VALIDATION")
    if dataset_type == "local":
        required_local = ["region", "province", "municipality", "barangay", "precinct_id"]
        missing_fields = [field for field in required_local if field not in df.columns]
        if missing_fields:
            validation.append(f"   [ERROR] Missing LOCAL fields: {', '.join(missing_fields)}")
        else:
            validation.append("   [OK] All required LOCAL fields present")
    else:
        required_overseas = ["region", "country", "jurisdiction", "precinct_id"]
        missing_fields = [field for field in required_overseas if field not in df.columns]
        if missing_fields:
            validation.append(f"   [ERROR] Missing OVERSEAS fields: {', '.join(missing_fields)}")
        else:
            validation.append("   [OK] All required OVERSEAS fields present")

        if "barangay" in df.columns:
            validation.append("   [WARNING] Overseas data contains 'barangay' field (unexpected)")
        else:
            validation.append("   [OK] No barangay field (expected for overseas)")

    return validation


print("=" * 70)
print("COMELEC 2025 DATA VALIDATION")
print("=" * 70)

df_local = load_dataset("output/combined_local.csv")
df_overseas = load_dataset("output/combined_overseas.csv")

report = []
report.append("=" * 70)
report.append("DATA VALIDATION REPORT: combined_local.csv (LOCAL)")
report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
report.append("=" * 70)
report.extend(validate_data(df_local, "local"))

report.append("\n" + "=" * 70)
report.append("DATA VALIDATION REPORT: combined_overseas.csv (OVERSEAS)")
report.append("=" * 70)
report.extend(validate_data(df_overseas, "overseas"))

full_report = "\n".join(report)
print(full_report)

with open("output/validation_report.txt", "w", encoding="utf-8") as f:
    f.write(full_report)

print("\n[OK] Report saved to output/validation_report.txt")