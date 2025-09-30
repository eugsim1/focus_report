---

# OCI FOCUS Reports Downloader

A Go-based utility to list and download **FOCUS reports** from Oracle Cloud Infrastructure (OCI) Object Storage. The program supports concurrent downloads, automatic file naming based on report dates, and generates a detailed CSV report of downloaded files.

---

## Features

* Lists FOCUS reports from OCI Object Storage based on a configurable number of past days.
* Supports concurrent downloads using a worker pool to improve speed.
* Automatically prefixes downloaded files with report date (`YYYYMMDD_`).
* Skips already downloaded files to avoid duplication.
* Generates a detailed **download operation report** in CSV format.
* Generates a **summary CSV** of all FOCUS reports with sizes and dates.
* Configurable via command-line flags.

---

## Prerequisites

* Go 1.20+ installed: [https://golang.org/dl/](https://golang.org/dl/)
* OCI Go SDK v65: `github.com/oracle/oci-go-sdk/v65`
* OCI configuration file (`~/.oci/config`) with appropriate credentials and tenancy access.

---

## Installation

Clone the repository:

```bash
git clone https://github.com/eugsim1/focus_report.git
cd oci-focus-downloader
```

Build the executable:

```bash
go build -o oci_focus_download list_FOCUS_pagination_V0-4.go
```

This produces an executable named `oci_focus_download` (or `oci_focus_download.exe` on Windows).

---

## Usage

```bash
./oci_focus_download \
    -workers 8 \
    -days 7 \
    -download ./downloads \
    -report download_report.csv
```

### Command-line Flags

| Flag        | Description                                 | Default               |
| ----------- | ------------------------------------------- | --------------------- |
| `-workers`  | Number of concurrent download workers       | 4                     |
| `-days`     | Number of past days to include in report    | 7                     |
| `-download` | Folder to download reports (optional)       | "" (skip download)    |
| `-report`   | CSV file name for download operation report | `download_report.csv` |

---

## Output

### 1. Downloaded Files

* Saved in the folder specified by `-download`.
* Filenames are prefixed with report date:

  ```
  YYYYMMDD_original_filename.ext
  ```

### 2. Download Operation Report (CSV)

Columns:

* `file_name` – downloaded filename
* `file_size` – size in bytes
* `report_date` – report date extracted from object name
* `status` – Success / Failed / Already exists
* `downloaded` – `true` if downloaded in this run
* `error` – error message if failed
* `last_attempt` – timestamp of last download attempt

Example:

```csv
file_name,file_size,report_date,status,downloaded,error,last_attempt
20250925_FOCUS_REPORT1.csv,12345,2025-09-25,Success,true,,2025-09-30T10:15:30Z
```

### 3. Summary CSV (`oci_focus_reports.csv`)

* Bucket name, object name, size in bytes, report date, tenancy OCID.
* Sorted by report date descending.

---

## Implementation Details

* Uses **OCI Go SDK v65** to interact with Object Storage.
* Extracts date from object path to generate prefixed filenames.
* Uses a **worker pool** with configurable concurrency.
* Skips files already downloaded.
* Handles errors gracefully and logs warnings for objects with invalid date formats.
* Generates CSV reports for easy auditing and tracking of downloads.

---

## Example Output

```
Found 42 FOCUS reports in bucket ocid1.bucket.oc1..example
Starting 8 workers to process 42 files...
✓ FOCUS_REPORT1.csv → 20250925_FOCUS_REPORT1.csv (12345 bytes)
✓ FOCUS_REPORT2.csv → 20250925_FOCUS_REPORT2.csv (23456 bytes)
Download completed in 12.345s
Download operation report generated: download_report.csv
Reports downloaded successfully to folder: ./downloads
CSV file generated successfully: oci_focus_reports.csv (42 reports)
```

---

## Disclaimer
This code is not supported by Oracle Corp, you can use and modify this code at your own risks

---
