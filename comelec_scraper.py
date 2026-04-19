#!/usr/bin/env python3
import json, time, csv, logging, sys, argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from threading import Lock

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

# =============================================================================
# CONFIGURATION
# =============================================================================

# Concurrency tuning
MUN_WORKERS      = 6    # municipalities scraped in parallel (configurable)
PRECINCT_WORKERS = 20   # precinct probes in parallel per municipality
BATCH_SIZE       = 20   # precinct IDs probed per batch before checking for all-404
MAX_SEQ          = 500
DELAY            = 0.05 # seconds between individual requests

# Retry settings for municipalities
MUN_MAX_RETRIES  = 3    # how many times to retry a failed municipality
MUN_RETRY_DELAY  = 5    # seconds to wait between retries

# Retry settings for geo fetch (handles 403 errors)
GEO_MAX_RETRIES  = 3    # how many times to retry a failed geo fetch
GEO_RETRY_DELAY  = 10   # seconds to wait between geo retries

# Output
OUTPUT_DIR_LOCAL    = Path("output/local")      # local regions CSVs
OUTPUT_DIR_OVERSEAS = Path("output/overseas")   # overseas regions CSVs
CACHE_DIR           = Path("comelec_cache")     # shared cache

# =============================================================================

# Local regions (Philippines)
LOCAL_REGIONS = {
    "BARMM":      "R0BARMM",
    "CAR":        "R0CAR00",
    "NCR":        "R0NCR00",
    "NIR":        "R00NIR0",
    "LAV":        "R00LAV0",
    "I":          "R001000",
    "II":         "R002000",
    "III":        "R003000",
    "V":          "R005000",
    "VI":         "R006000",
    "VII":        "R007000",
    "VIII":       "R008000",
    "IX":         "R009000",
    "X":          "R010000",
    "XI":         "R011000",
    "XII":        "R012000",
    "XIII":       "R013000",
    "IVA":        "R04A000",
    "IVB":        "R04B000",
}

# Overseas absentee voters regions
OVERSEAS_REGIONS = {
    "ASIA_PACIFIC":            "9000000",
    "NORTH_AND_LATIN_AMERICAS": "9100000",
    "MIDDLE_EAST_AND_AFRICAS":  "9200000",
    "EUROPE":                   "9300000",
}

BASE_URL = "https://2025electionresults.comelec.gov.ph/data"
SITE_URL = "https://2025electionresults.comelec.gov.ph/er-result"

CSV_FIELDS_LOCAL = [
    "region", "province", "municipality", "barangay", "voting_center",
    "precinct_id", "precinct_in_cluster",
    "registered_voters", "actual_voters", "valid_ballots", "turnout_pct",
    "contest_type", "contest_code", "contest_name",
    "candidate", "votes", "percentage",
]

CSV_FIELDS_OVERSEAS = [
    "region", "country", "jurisdiction", "voting_center",
    "precinct_id", "precinct_in_cluster",
    "registered_voters", "actual_voters", "valid_ballots", "turnout_pct",
    "contest_type", "contest_code", "contest_name",
    "candidate", "votes", "percentage",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# Thread-safe row accumulator (reset between regions)
all_rows: list[dict] = []
rows_lock = Lock()

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept":     "application/json, text/plain, */*",
    "Referer":    "https://2025electionresults.comelec.gov.ph/",
    "Origin":     "https://2025electionresults.comelec.gov.ph",
})

# =============================================================================
# COOKIE BOOTSTRAP
# =============================================================================

def bootstrap_cookies():
    if not SELENIUM_AVAILABLE:
        return
    opts = Options()
    for a in ["--headless=new", "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]:
        opts.add_argument(a)
    driver = None
    try:
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            svc = Service(ChromeDriverManager().install())
        except ImportError:
            svc = Service()
        driver = webdriver.Chrome(service=svc, options=opts)
        driver.get(SITE_URL)
        time.sleep(6)
        for c in driver.get_cookies():
            session.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))
        log.info(f"✓ {len(driver.get_cookies())} cookies transferred.")
    except Exception as e:
        log.error(f"Cookie error: {e}")
    finally:
        if driver:
            try: driver.quit()
            except: pass

# =============================================================================
# GEO FETCHING (shared cache)
# =============================================================================

def fetch_geo(code: str, retry_count: int = 0) -> dict | None:
    """Fetch geo data with retry logic for 403 errors."""
    # Determine if this is overseas (codes starting with 9) or local
    path_type = "overseas" if code.startswith("9") else "local"
    
    cache = CACHE_DIR / "regions" / path_type / f"{code}.json"
    if cache.exists():
        try:
            data = json.loads(cache.read_text(encoding="utf-8"))
            if data.get("regions"):
                return data
            cache.unlink(missing_ok=True)
        except:
            cache.unlink(missing_ok=True)
    
    url = f"{BASE_URL}/regions/{path_type}/{code}.json"
    try:
        r = session.get(url, timeout=30)
        
        # Handle 403 Forbidden with retry and cookie refresh
        if r.status_code == 403:
            if retry_count < GEO_MAX_RETRIES:
                log.warning(f"fetch_geo({code}): 403 Forbidden (attempt {retry_count + 1}/{GEO_MAX_RETRIES + 1}) — refreshing cookies")
                bootstrap_cookies()  # Refresh cookies
                time.sleep(GEO_RETRY_DELAY)
                return fetch_geo(code, retry_count + 1)
            else:
                log.error(f"fetch_geo({code}): 403 Forbidden after {GEO_MAX_RETRIES + 1} attempts — giving up")
                return None
        
        r.raise_for_status()
        data = r.json()
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps(data), encoding="utf-8")
        time.sleep(DELAY)
        return data
    except requests.HTTPError as e:
        if e.response.status_code == 403:
            # Already handled above
            return None
        log.warning(f"fetch_geo({code}): HTTP {e.response.status_code} - {e}")
    except Exception as e:
        log.warning(f"fetch_geo({code}): {e}")
    return None

# =============================================================================
# ELECTION RESULT FETCHING
# =============================================================================

def fetch_er(precinct_id: str) -> tuple[dict | None, int]:
    prefix = precinct_id[:3]
    cache  = CACHE_DIR / "er" / prefix / f"{precinct_id}.json"
    if cache.exists():
        try: return json.loads(cache.read_text(encoding="utf-8")), 200
        except: cache.unlink(missing_ok=True)
    url = f"{BASE_URL}/er/{prefix}/{precinct_id}.json"
    try:
        r = session.get(url, timeout=30)
        if r.status_code == 404: return None, 404
        r.raise_for_status()
        data = r.json()
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps(data), encoding="utf-8")
        time.sleep(DELAY)
        return data, 200
    except requests.HTTPError as e:
        return None, e.response.status_code
    except Exception as e:
        log.warning(f"fetch_er({precinct_id}): {e}")
        return None, 0

# =============================================================================
# ROW BUILDING
# =============================================================================

def build_rows(er: dict, region_name: str, province_name: str, mun_name: str) -> list[dict]:
    info     = er.get("information") or {}
    loc      = [p.strip() for p in info.get("location", "").split(",")]
    barangay = loc[3] if len(loc) >= 4 else ""
    ctx = {
        "region":              region_name,
        "province":            province_name,
        "municipality":        mun_name,
        "barangay":            barangay,
        "voting_center":       info.get("votingCenter", ""),
        "precinct_id":         info.get("precinctId", ""),
        "precinct_in_cluster": info.get("precinctInCluster", ""),
        "registered_voters":   info.get("numberOfRegisteredVoters", ""),
        "actual_voters":       info.get("numberOfActuallyVoters", ""),
        "valid_ballots":       info.get("numberOfValidBallot", ""),
        "turnout_pct":         info.get("turnout", ""),
    }
    rows = []
    for ctype in ("national", "local"):
        for contest in er.get(ctype) or []:
            cands = contest.get("candidates") or {}
            if isinstance(cands, dict): cands = cands.get("candidates") or []
            for cand in cands:
                rows.append({**ctx,
                    "contest_type": ctype,
                    "contest_code": contest.get("contestCode", ""),
                    "contest_name": contest.get("contestName", ""),
                    "candidate":    cand.get("name", ""),
                    "votes":        cand.get("votes", 0),
                    "percentage":   cand.get("percentage", ""),
                })
    return rows

# =============================================================================
# MUNICIPALITY SCRAPING (with retry)
# =============================================================================

def scrape_municipality(mun_code: str, mun_name: str, province_name: str, region_name: str):
    """Scrape one municipality with up to MUN_MAX_RETRIES attempts. Raises on final failure."""
    last_exc = None
    for attempt in range(1, MUN_MAX_RETRIES + 1):
        try:
            _scrape_municipality_once(mun_code, mun_name, province_name, region_name)
            return  # success
        except Exception as e:
            last_exc = e
            log.warning(
                f"  ✗ {mun_name} attempt {attempt}/{MUN_MAX_RETRIES} failed: {e}"
                + (f" — retrying in {MUN_RETRY_DELAY}s" if attempt < MUN_MAX_RETRIES else " — giving up")
            )
            if attempt < MUN_MAX_RETRIES:
                time.sleep(MUN_RETRY_DELAY)
    raise RuntimeError(f"Municipality '{mun_name}' ({mun_code}) failed after {MUN_MAX_RETRIES} attempts") from last_exc


def _scrape_municipality_once(mun_code: str, mun_name: str, province_name: str, region_name: str):
    prefix4 = mun_code[:4]
    found   = 0
    seq     = 1

    log.info(f"    → Scraping {mun_name} ({mun_code})")

    while seq <= MAX_SEQ:
        batch_ids = [f"{prefix4}{s:04d}" for s in range(seq, min(seq + BATCH_SIZE, MAX_SEQ + 1))]
        seq += BATCH_SIZE

        results = {}
        with ThreadPoolExecutor(max_workers=PRECINCT_WORKERS) as ex:
            futures = {ex.submit(fetch_er, pid): pid for pid in batch_ids}
            for f in as_completed(futures):
                pid = futures[f]
                try:
                    results[pid] = f.result()
                except Exception as e:
                    results[pid] = (None, 0)

        batch_found = 0
        for pid in batch_ids:
            er, status = results.get(pid, (None, 0))
            if status == 0:
                # Network-level failure — treat as a hard error to trigger retry
                raise ConnectionError(f"Network error fetching precinct {pid}")
            if status == 200 and er:
                batch_found += 1
                found += 1
                rows = build_rows(er, region_name, province_name, mun_name)
                with rows_lock:
                    all_rows.extend(rows)
                log.info(f"      ✓ {pid}  {er.get('information',{}).get('location','')}  rows={len(rows)}")

        if batch_found == 0:
            log.info(f"    ✓ {mun_name}: done ({found} precincts found)")
            break

    if found == 0:
        log.warning(f"    ✗ {mun_name}: no precincts found — check prefix")

# =============================================================================
# PROVINCE SCRAPING
# =============================================================================

def scrape_province(province_code: str, province_name: str, region_name: str) -> bool:
    """Scrape a province. Returns True on success, False if province has no data available."""
    log.info(f"  ┌ Province: {province_name} ({province_code})")
    data = fetch_geo(province_code)
    if not data:
        log.warning(f"  └ Province {province_name}: no data available (skipping)")
        return False

    municipalities = [
        {"code": str(r["code"]), "name": r.get("name", "")}
        for r in (data.get("regions") or [])
    ]
    if not municipalities:
        log.warning(f"  └ Province {province_name}: no municipalities found (skipping)")
        return False

    log.info(f"  │ {len(municipalities)} municipalities — scraping {MUN_WORKERS} at a time")

    with ThreadPoolExecutor(max_workers=MUN_WORKERS) as ex:
        futures = {
            ex.submit(scrape_municipality, m["code"], m["name"], province_name, region_name): m["name"]
            for m in municipalities
        }
        for f in as_completed(futures):
            mun_name = futures[f]
            try:
                f.result()
            except Exception as e:
                # Municipality failed all retries — log but continue with other municipalities
                log.error(f"  │ ✗ {mun_name}: {e}")

    log.info(f"  └ Province {province_name}: complete")
    return True

# =============================================================================
# CSV EXPORT
# =============================================================================

def region_csv_path(region_key: str, region_code: str, is_overseas: bool) -> Path:
    output_dir = OUTPUT_DIR_OVERSEAS if is_overseas else OUTPUT_DIR_LOCAL
    return output_dir / f"region_{region_key}_{region_code}.csv"


def export_region_csv(region_key: str, region_code: str, is_overseas: bool):
    if not all_rows:
        log.warning(f"No rows collected for region {region_key} — skipping CSV export")
        return

    # Use appropriate fields and sort key based on region type
    csv_fields = CSV_FIELDS_OVERSEAS if is_overseas else CSV_FIELDS_LOCAL
    
    if is_overseas:
        # Overseas: sort without barangay, rename keys for export
        sorted_rows = sorted(
            all_rows,
            key=lambda r: (
                r["province"],
                r["municipality"],
                r["precinct_id"],
                r["contest_type"],   # national before local
                r["contest_name"],
                r["candidate"],
            )
        )
        # Rename keys: province → country, municipality → jurisdiction
        export_rows = [
            {
                "region": r["region"],
                "country": r["province"],
                "jurisdiction": r["municipality"],
                "voting_center": r["voting_center"],
                "precinct_id": r["precinct_id"],
                "precinct_in_cluster": r["precinct_in_cluster"],
                "registered_voters": r["registered_voters"],
                "actual_voters": r["actual_voters"],
                "valid_ballots": r["valid_ballots"],
                "turnout_pct": r["turnout_pct"],
                "contest_type": r["contest_type"],
                "contest_code": r["contest_code"],
                "contest_name": r["contest_name"],
                "candidate": r["candidate"],
                "votes": r["votes"],
                "percentage": r["percentage"],
            }
            for r in sorted_rows
        ]
    else:
        # Local: sort with barangay
        sorted_rows = sorted(
            all_rows,
            key=lambda r: (
                r["province"],
                r["municipality"],
                r["barangay"],
                r["precinct_id"],
                r["contest_type"],   # national before local
                r["contest_name"],
                r["candidate"],
            )
        )
        export_rows = sorted_rows

    out_path = region_csv_path(region_key, region_code, is_overseas)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=csv_fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(export_rows)

    log.info(f"✓ {len(all_rows):,} rows → {out_path}")

# =============================================================================
# REGION SCRAPING
# =============================================================================

def scrape_region(region_key: str, region_code: str, is_overseas: bool) -> bool:
    """
    Scrape all provinces in a region sequentially.
    Returns True on success, False on failure.
    """
    global all_rows

    csv_path = region_csv_path(region_key, region_code, is_overseas)
    if csv_path.exists():
        log.info(f"⏭  Region {region_key}: CSV already exists ({csv_path}) — skipping")
        return True

    region_type = "OVERSEAS" if is_overseas else "LOCAL"
    log.info(f"\n{'='*60}")
    log.info(f"  REGION: {region_key} ({region_type})  ({region_code})")
    log.info(f"{'='*60}")

    # Dynamically discover provinces for this region
    data = fetch_geo(region_code)
    if not data:
        log.error(f"✗ Region {region_key}: failed to fetch geo data")
        return False

    provinces = sorted(
        [{"code": str(r["code"]), "name": r.get("name", "")} for r in (data.get("regions") or [])],
        key=lambda p: p["name"]  # alphabetical province order
    )

    if not provinces:
        log.error(f"✗ Region {region_key}: no provinces found")
        return False

    log.info(f"  Found {len(provinces)} provinces: {[p['name'] for p in provinces]}\n")

    # Reset accumulator for this region
    all_rows = []

    # Track province results
    province_results = []
    for province in provinces:
        try:
            success = scrape_province(province["code"], province["name"], region_key)
            province_results.append((province["name"], success))
        except Exception as e:
            log.error(f"  ✗ Province {province['name']}: unexpected error: {e}")
            province_results.append((province["name"], False))

    # Summary
    successful = sum(1 for _, success in province_results if success)
    failed = len(province_results) - successful
    log.info(f"\n  Province summary: {successful} succeeded, {failed} failed/skipped")

    if successful == 0:
        log.error(f"✗ Region {region_key}: no provinces successfully scraped")
        return False

    export_region_csv(region_key, region_code, is_overseas)
    return True

# =============================================================================
# CLI HELPERS
# =============================================================================

def list_available_regions(region_type: str = "all"):
    """Display available regions."""
    print("\n" + "="*70)
    print("  AVAILABLE REGIONS")
    print("="*70)
    
    if region_type in ("all", "local"):
        print("\n  LOCAL REGIONS (Philippines):")
        print("  " + "-"*66)
        for key in sorted(LOCAL_REGIONS.keys()):
            code = LOCAL_REGIONS[key]
            csv_path = region_csv_path(key, code, is_overseas=False)
            status = "✓ scraped" if csv_path.exists() else ""
            print(f"  {key:15s} ({code})  {status}")
    
    if region_type in ("all", "overseas"):
        print("\n  OVERSEAS ABSENTEE VOTERS:")
        print("  " + "-"*66)
        for key in sorted(OVERSEAS_REGIONS.keys()):
            code = OVERSEAS_REGIONS[key]
            csv_path = region_csv_path(key, code, is_overseas=True)
            status = "✓ scraped" if csv_path.exists() else ""
            print(f"  {key:25s} ({code})  {status}")
    
    print("="*70 + "\n")


def parse_regions(region_args: list[str], region_type: str) -> list[tuple[str, str, bool]]:
    """
    Parse region arguments and return list of (region_key, region_code, is_overseas) tuples.
    Handles special keywords: 'all', 'list'
    """
    if not region_args:
        return []
    
    # Check for special keywords
    if "list" in [r.lower() for r in region_args]:
        list_available_regions(region_type)
        sys.exit(0)
    
    # Determine which region sets to use
    if region_type == "all":
        all_regions = {
            **{k: (v, False) for k, v in LOCAL_REGIONS.items()},
            **{k: (v, True) for k, v in OVERSEAS_REGIONS.items()}
        }
    elif region_type == "overseas":
        all_regions = {k: (v, True) for k, v in OVERSEAS_REGIONS.items()}
    else:  # local
        all_regions = {k: (v, False) for k, v in LOCAL_REGIONS.items()}
    
    if "all" in [r.lower() for r in region_args]:
        return [(k, code, is_overseas) for k, (code, is_overseas) in all_regions.items()]
    
    # Parse individual regions
    regions_to_scrape = []
    invalid_regions = []
    
    for region_arg in region_args:
        region_upper = region_arg.upper()
        if region_upper in all_regions:
            code, is_overseas = all_regions[region_upper]
            regions_to_scrape.append((region_upper, code, is_overseas))
        else:
            invalid_regions.append(region_arg)
    
    if invalid_regions:
        print(f"\n❌ Invalid region(s): {', '.join(invalid_regions)}")
        print(f"\nUse '--regions list' to see available regions for --type {region_type}\n")
        sys.exit(1)
    
    return regions_to_scrape

# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Scrape COMELEC 2025 election results for local and overseas regions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all regions
  python comelec_data_scraper5.py --regions list
  
  # List only local regions
  python comelec_data_scraper5.py --type local --regions list
  
  # Scrape specific local regions
  python comelec_data_scraper5.py --regions VII NCR
  python comelec_data_scraper5.py --type local --regions VII NCR CAR
  
  # Scrape specific overseas regions
  python comelec_data_scraper5.py --type overseas --regions ASIA_PACIFIC EUROPE
  
  # Scrape all local regions
  python comelec_data_scraper5.py --type local --regions all
  
  # Scrape all overseas regions
  python comelec_data_scraper5.py --type overseas --regions all
  
  # Scrape everything (local + overseas)
  python comelec_data_scraper5.py --type all --regions all
        """
    )
    
    parser.add_argument(
        "--type",
        choices=["local", "overseas", "all"],
        default="local",
        help="Type of regions to scrape: local (Philippines), overseas (OAV), or all (default: local)"
    )
    
    parser.add_argument(
        "--regions",
        nargs="+",
        metavar="REGION",
        help="Region(s) to scrape. Use 'all' for all regions of the selected type, 'list' to show available regions."
    )
    
    args = parser.parse_args()
    
    # If no arguments provided, show help and list regions
    if not args.regions:
        parser.print_help()
        print()
        list_available_regions(args.type)
        return
    
    # Parse and validate regions
    regions_to_scrape = parse_regions(args.regions, args.type)
    
    if not regions_to_scrape:
        print("\n❌ No regions specified.")
        parser.print_help()
        return
    
    # Setup
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR_LOCAL.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR_OVERSEAS.mkdir(parents=True, exist_ok=True)
    
    bootstrap_cookies()
    
    # Display scraping plan
    local_count = sum(1 for _, _, is_overseas in regions_to_scrape if not is_overseas)
    overseas_count = sum(1 for _, _, is_overseas in regions_to_scrape if is_overseas)
    
    print("\n" + "="*60)
    print(f"  SCRAPING {len(regions_to_scrape)} REGION(S)")
    if local_count > 0:
        print(f"  • {local_count} Local (Philippines)")
    if overseas_count > 0:
        print(f"  • {overseas_count} Overseas (OAV)")
    print("="*60)
    
    for region_key, region_code, is_overseas in regions_to_scrape:
        region_type = "Overseas" if is_overseas else "Local"
        print(f"  • {region_key} ({region_type})")
    print("="*60 + "\n")
    
    # Scrape regions
    results: dict[str, bool] = {}
    for region_key, region_code, is_overseas in regions_to_scrape:
        success = scrape_region(region_key, region_code, is_overseas)
        results[region_key] = success
    
    # Final summary
    print(f"\n{'='*60}")
    print("  SCRAPE SUMMARY")
    print(f"{'='*60}")
    for region_key, success in results.items():
        status = "✓" if success else "✗"
        print(f"  {status}  {region_key}")
    
    passed = sum(results.values())
    failed = len(results) - passed
    print(f"\n  {passed} succeeded, {failed} failed")
    print(f"  Local CSVs:    {OUTPUT_DIR_LOCAL}/")
    print(f"  Overseas CSVs: {OUTPUT_DIR_OVERSEAS}/")


if __name__ == "__main__":
    main()
