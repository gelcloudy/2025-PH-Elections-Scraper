# COMELEC 2025 Election Results Scraper

**Author:** Mark Gelson Panganoron

A high-performance, multi-threaded Python scraper for collecting election results from the COMELEC (Commission on Elections) 2025 Philippine election results website. Supports both local (Philippines) and overseas absentee voter (OAV) regions.

Link to Comelec Website: https://2025electionresults.comelec.gov.ph/er-result

Please note that the COMELEC website structure can vary from one election cycle to another. This scraper is built for the current structure and may require updates for future elections. 

## Features

- **Multi-threaded scraping** - Parallel processing of municipalities and precincts
- **Smart caching** - Avoids redundant API calls with local file caching
- **Retry mechanism** - Automatic retries for failed requests with exponential backoff
- **Cookie management** - Automated Selenium-based cookie bootstrapping (optional)
- **Comprehensive data** - Captures all national and local contests with full candidate details
- **CSV export** - Clean, structured output files per region
- **Progress tracking** - Real-time logging of scraping progress
- **Robust error handling** - Handles 403, 404, and network errors gracefully

## Requirements

- Python 3.7+
- Chrome/Chromium browser (for cookie management)

## Installation

1. Clone or download this repository
2. Install dependencies:
   ```bash
   pip install requests selenium webdriver-manager
   ```

## Configuration

Key configuration parameters are defined at the top of `comelec_scraper.py`:

```python
# Concurrency settings
MUN_WORKERS      = 6    # Municipalities scraped in parallel
PRECINCT_WORKERS = 20   # Precinct probes in parallel per municipality
BATCH_SIZE       = 20   # Precinct IDs probed per batch
MAX_SEQ          = 500  # Maximum precinct sequence number per municipality

# Retry settings
MUN_MAX_RETRIES  = 3    # Municipality retry attempts
MUN_RETRY_DELAY  = 5    # Delay between municipality retries (seconds)
GEO_MAX_RETRIES  = 3    # Geo fetch retry attempts
GEO_RETRY_DELAY  = 10   # Delay between geo retries (seconds)

# Timing
DELAY = 0.05  # Seconds between individual requests
```

Adjust these values based on your network conditions and desired performance.

## Usage

### Basic Command Structure

```bash
python comelec_scraper.py --type <TYPE> --regions <REGIONS>
```

### List Available Regions

```bash
# List all regions
python comelec_scraper.py --regions list

# List only local regions
python comelec_scraper.py --type local --regions list

# List only overseas regions
python comelec_scraper.py --type overseas --regions list
```

### Scrape Specific Regions

```bash
# Scrape specific local regions
python comelec_scraper.py --regions VII NCR CAR

# Scrape specific overseas regions
python comelec_scraper.py --type overseas --regions ASIA_PACIFIC EUROPE
```

### Scrape All Regions

```bash
# Scrape all local regions
python comelec_scraper.py --type local --regions all

# Scrape all overseas regions
python comelec_scraper.py --type overseas --regions all

# Scrape everything (local + overseas)
python comelec_scraper.py --type all --regions all
```

## Available Regions

### Local Regions (19 regions - Philippines)
- **BARMM** - Bangsamoro Autonomous Region in Muslim Mindanao
- **CAR** - Cordillera Administrative Region
- **NCR** - National Capital Region
- **NIR** - Negros Island Region
- **LAV** - Local Absentee Voting
- **I, II, III** - Regions 1, 2, 3
- **IVA** - Region IV-A (CALABARZON)
- **IVB** - Region IV-B (MIMAROPA)
- **V, VI, VII, VIII, IX, X, XI, XII, XIII** - Regions 5-13

### Overseas Regions
- **ASIA_PACIFIC** - Asia-Pacific Region
- **NORTH_AND_LATIN_AMERICAS** - North and Latin Americas
- **MIDDLE_EAST_AND_AFRICAS** - Middle East and Africa
- **EUROPE** - Europe

## Output Format

### Directory Structure
```
output/
├── local/
│   ├── region_NCR_R0NCR00.csv
│   ├── region_CAR_R0CAR00.csv
│   └── ...
└── overseas/
    ├── region_ASIA_PACIFIC_9000000.csv
    └── ...
```

### CSV Fields - Local Regions
- `region` - Region code
- `province` - Province name
- `municipality` - Municipality/city name
- `barangay` - Barangay name
- `voting_center` - Voting center name
- `precinct_id` - Precinct identifier
- `precinct_in_cluster` - Precinct number within cluster
- `registered_voters` - Total registered voters
- `actual_voters` - Number of voters who voted
- `valid_ballots` - Number of valid ballots
- `turnout_pct` - Voter turnout percentage
- `contest_type` - "national" or "local"
- `contest_code` - Contest identifier code
- `contest_name` - Full contest name (e.g., "President and Vice President")
- `candidate` - Candidate name
- `votes` - Number of votes received
- `percentage` - Percentage of votes

### CSV Fields - Overseas Regions
Similar to local regions, but with:
- `country` - Country name (instead of province)
- `jurisdiction` - Jurisdiction name (instead of municipality)
- No `barangay` field

## How It Works

### 1. Cookie Bootstrapping
The scraper uses Selenium to visit the COMELEC website and extract session cookies, which are then transferred to the requests session. This helps avoid 403 Forbidden errors.

### 2. Geographic Data Fetching
The scraper fetches hierarchical geographic data:
- **Region** → **Provinces** → **Municipalities**
  
Geographic data is cached in `comelec_cache/regions/` to avoid redundant API calls.

### 3. Precinct Discovery
For each municipality, the scraper probes precinct IDs using a 4-digit sequence number:
- Format: `{municipality_code}{sequence:04d}`
- Example: `0101` + `0001` = `01010001`
- Probes are done in batches of 20 precincts in parallel
- Stops when an entire batch returns 404 (no more precincts)

### 4. Election Results Fetching
For each discovered precinct, the scraper:
1. Checks the cache (`comelec_cache/er/`)
2. If not cached, fetches from the API
3. Parses the JSON response to extract:
   - Precinct information (location, voters, turnout)
   - All national contests (President, VP, Senators, etc.)
   - All local contests (Governor, Mayor, etc.)
   - All candidates and their votes

### 5. Data Aggregation & Export
Results are accumulated in memory and exported to CSV files, sorted by:
- Province → Municipality → Barangay → Precinct → Contest Type → Contest → Candidate

## Cache Structure

The scraper maintains a local cache to speed up subsequent runs:

```
comelec_cache/
├── regions/
│   ├── local/
│   │   └── {region_code}.json
│   └── overseas/
│       └── {region_code}.json
└── er/
    ├── 010/
    │   └── {precinct_id}.json
    ├── 011/
    └── ...
```

## Performance

- **Concurrency:** 6 municipalities scraped simultaneously by default
- **Precinct scanning:** 20 precincts probed in parallel per municipality
- **Network throttling:** 50ms delay between requests
- **Caching:** Avoids re-fetching already downloaded data
- **Smart batching:** Stops scanning when no more precincts are found

## Error Handling

### 403 Forbidden Errors
- Triggers automatic cookie refresh via Selenium
- Retries up to 3 times with 10-second delays
- Logs warnings and continues with other regions if all retries fail

### Network Errors
- Treats network failures as hard errors to trigger municipality retry
- Up to 3 retry attempts per municipality with 5-second delays
- Continues with other municipalities if retries exhausted

### 404 Not Found
- Expected response when no precinct exists at a sequence number
- Used to determine when to stop probing a municipality

## Troubleshooting

### Issue: 403 Forbidden errors
**Solution:** Ensure Selenium and webdriver-manager are installed. The scraper will automatically refresh cookies.

### Issue: Slow scraping
**Solutions:**
- Increase `MUN_WORKERS` (municipalities in parallel)
- Increase `PRECINCT_WORKERS` (precincts in parallel)
- Reduce `DELAY` (but may trigger rate limiting)

### Issue: Missing data
**Solutions:**
- Check if the region code is correct using `--regions list`
- Verify the COMELEC website has published results for that region
- Check logs for specific error messages

### Issue: Chrome driver errors
**Solution:** 
- Ensure Chrome/Chromium is installed
- Update selenium: `pip install --upgrade selenium webdriver-manager`
- If issues persist, the scraper can run without Selenium (fewer cookies available)

## Example Output

```
==============================================================
  REGION: NCR (LOCAL)  (R0NCR00)
==============================================================
  Found 17 provinces: ['CITY OF MANILA', 'CALOOCAN CITY', ...]

  ┌ Province: CITY OF MANILA (1390000)
  │ 6 municipalities — scraping 6 at a time
    → Scraping PACO (1390005)
      ✓ 13900050001  MANILA, PACO, BARANGAY 686, JOSE ABAD SANTOS ELEMENTARY SCHOOL  rows=245
      ✓ 13900050002  MANILA, PACO, BARANGAY 686, JOSE ABAD SANTOS ELEMENTARY SCHOOL  rows=245
    ✓ PACO: done (42 precincts found)
  └ Province CITY OF MANILA: complete

✓ 125,384 rows → output\local\region_NCR_R0NCR00.csv
```

## License & Disclaimer

This project's source code is licensed under the MIT License.

The data used is sourced from publicly accessible government websites and is **not owned by this project**. Under the Intellectual Property Code of the Philippines (RA 8293, Sections 175–176), such works are generally not subject to copyright; however, users remain responsible for complying with the source agency’s terms.

- No ownership is claimed over the data
- The data remains with the respective government agency
- Commercial use of the data may require permission from the appropriate agency
- Users must comply with the website’s Terms of Service and avoid misuse

This project is not affiliated with any government agency.

Use at your own risk.

## Contributions

Contributions are welcome. For any discrepancies or improvements, please open an issue or submit a pull request

## Contact

- **Author:** Mark Gelson Panganoron 
- **Github:** https://github.com/gelcloudy 
- **LinkedIn:** https://www.linkedin.com/in/gelsonpanganoron 

---

**Last Updated:** April 2026
