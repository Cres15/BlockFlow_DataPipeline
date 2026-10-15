# Data Extraction Documentation

1. Data Source and Extraction Specification
  1. Source System
Name: BlockFlow: Design and Development of an Integrated Inventory, and Sales Management System for Magalin Hollow Blocks Trading
Purpose: To monitor and record the daily operation of the business in Magalin Hollow Blocks Trading, including a sales transaction, stocks updates, and operational expenses. The Owner and Staff can record daily and the data is exported as raw CSV into the entry directory, where it serves as the source input for the BlockFlow ETL pipeline which prepares it for output in analytics.

  2. Source Database or File
Database management system: SQLite is the pipeline storage like (blockflow.db), there is a serverless, and self-contained relational database that stores data in a local file. But, it is not the extraction source, it is the destination where extracted data is loaded into the users, inventory, expenses, and sales tables.
File format: The file is CSV for daily raw batch files (stocks updates, sales receipts, and expenses logs) in an entry directory serving as the extraction source.

  3. Extraction Method
Incremental: The pipeline performs a daily incremental extraction, pulling only to the most recent days batch files rather than re-extracting the entire dataset in every run. 
Extraction: The purpose of an extraction window is to determine the transaction date fields in the raw batch files, including in updated_at (last stocks updates), sales_date (sales receipts), and expenses_date (expenses logs). Each schedule extracts only in records belonging to the previous calendar day and any already processed records are protected from duplication by the INSERT/UPSERT logic used at load time.
How data will be extracted: The extracted process is on each scheduled run, the Prefect flow triggers the extraction task, which scans the entry directory for the target days raw CSV batch files. Then the Python reads these files into in memory pandas DataFrames, which passed in the Transform stage of the pipeline.
Extraction tool: The Python extraction script using pandas, csv, and json libraries, wrapped as a Prefect @task inside a Prefect @flow. The Prefect handles the scheduling, task tracking, retries and execution logging.
Extraction schedule: 12:00 AM daily with a maximum of one active run at a time and only 30 minutes execution timeout.

 4.Extraction Scope
Data to be extracted: The pipeline extracts the business daily operation data. There are three domains: the stock updates (current stock levels of hollow blocks), sales transactions (hollow block sales receipts), and operational expenses (raw materials, maintenance, and other costs). This pipeline matches to define the data source of daily raw CSV batch operational logs.
Files: Per batch domain there is one daily entry in the entry directory, including the stock updates, sales receipts, and expense logs as (CSV).
Target tables: There are three tables including the Stocks, Sales, and Expenses in the SQLite database. This is extracted fields follow the documented schema, included: Stocks (size, quantity, unit, unit price, updated at, recorded by), Sales (customer name, phone number, shop name, quantity, unit price, sale date, recorded by), and Expenses (category, description, amount, expense date, recorded by).
Excluded data: The user account credentials are not extracted, since the users table exists only for system login and access control, not for operational analytics.

5.Source Limitations and Assumptions
The data extraction process is limited by the availability and quality of the daily raw CSV files. Some source data may contain missing values, duplicate records, or inconsistent date and number formats, which may require cleaning and checking before loading the data into the database. The extraction is also limited to the available operational data, including sales, stocks, and expense records. Some historical records may be incomplete because the business previously relied on manual logs and spreadsheets. The extraction process also depends on the defined database schema and relationships. Changes in field names, data types, or required fields may require adjustments to the extraction and transformation process. External information, such as competitor pricing and market costs, is outside the scope of the extraction.
The extraction process assumes that the required daily raw CSV files are available in the designated folder and that the system has the necessary read and write permissions. It also assumes that the local SQLite database is accessible and that the required tables and relationships are already available. The process also assumes that the extracted data follows the required format and contains valid references for fields such as recorded_by and stocks_id. It is also assumed that the extracted data represents the actual operational records of Magalin Hollow Blocks Trading and is processed only for the intended BlockFlow system and reporting purposes. Only authorized business data related to the BlockFlow system is included in the extraction.



3. Extraction Validation and Data Quality Checks
This stage is to define the checks used to verify that the extracted data is complete, reliable, and suitable for the Transform stage. The checks operate at the extraction level only in the data cleaning, standardization, and business transformations (drop duplicates, fill or remove null values, standardize date and number types) are documented in the Transformation Stage, consistent with the pipeline's data lineage. Each check is executed within the Prefect extraction flow, which tracks task states, catches errors, performs automated retries, and logs execution metrics in real time.

Check Name: Source Connection
Target: The entry directory path and the extraction tasks access to it.
Purpose: To verify that the source location is reachable before any file operations are attempted. If a directory level has a failure means an environment or configuration problem, and catching it to prevent the misleading file level errors.
Validation Criteria: The entry directory exists at the configured path and to the extraction process holds read permission. Then if the check fails if the path does

Check Name: File Accessibility 
Target: Each expected daily batch file (stocks updates, sales receipts, expense logs in CSV).
Purpose: To verify that the raw data files required for this extraction window is actually present and readable, since the pipelines declared dependency is the presence of raw log files in the entry directory.
Validation Criteria: The all expected files for the extraction window exist, are non-empty, and to prise in their declared format (CSV). The purpose of the check fails if any expected file is missing, empty, or indelible, with the affected filename written to the Prefect log.

Check Name: Columns Exist
Target: The column headers of each extracted batch file, checked against the documented schema.
Purpose: The schema change in the exported files would silently break downstream loading, so the file structure must match the documented schema before the data is accepted into the pipeline.
Validation Criteria: Every required column for the domain is present to Stocks (size, quantity, unit, unit_price, updated_at), Sales (customer_name, phone_number, shop_name, quantity, unit_price, sales_date), Expenses (category, description, amount, expense_date). Then the check fails if any required column is missing or renamed.

Check Name: Present Identifiers
Target: The identifier columns recorded_by (all domains) and stock_id (sales records).
Purpose: These columns are the foreign keys that extracted records to users and stocks items, as validated in the pipeline design that records without not satisfying the relational constraints of the destination database.
Validation Criteria: recorded_by and stock_id that contain non-null values in every extracted record. The check fails if any record is missing an identifier value.

Check Name: Primary key uniqueness
Target: Extracted record within each batch, and key by the domain identifying combination.
Purpose: To duplicate rows inside a batch would produce duplicate records at load time and inflate sales totals and stock counts, even though the UPSERT logic only protects against re-extraction of the same batch.
Validation Criteria: Have zero duplicate records exist on the identifying key within each extracted batch. The check fails if any duplicate is detected.

Check Name: Record count Integrity
Target: Each extracted pandas DataFrame, is compared against the number of data records in its corresponding raw file.
Purpose: To confirm that each file was read in full rather than shortened. A shortened  read would silently drop records at the end of a file, and those lost of the stock updates, sales or expenses would never reach the database.
Validation Criteria: For every extracted file, the DataFrame's row count is greater than zero and exactly equals the number of data records counted in the raw file, with header rows excluded. Then the check fails on a zero-row result or any row-count mismatch, with the filename and both counts written to the Prefect log.

Check Name: Domain Completeness
Target: The set of extraction outputs for the scheduled run across the three operational domains.
Purpose: To confirm that the run processed all three data domains, so the Transform and Load stages receive a complete dataset for the day rather than a partial one covering only some tables.
Validation Criteria: All the three domains (stocks, sales, expenses) produced an extracted DataFrame in the run. Then the check fails if any domain is missing from the extraction results, naming the absent domain in the Prefect log.

Check Name: Execution Completion within Time Limit
Target: The total execution duration of the extraction flow and its final state.
Purpose: To detect interruptions and hung executions, an extraction that never finishes is as harmful as one that fails, because the schedule expects a completed run before the next day's batch arrives.
Validation Criteria: The extraction flow reaches a Completed state within the configured 30-minute execution timeout and with a maximum of one active run. The check fails if the timeout is exceeded, and the run is marked failed so it is not treated as successfully extracted data.

4. 
    ## Extraction Run Metadata

| Field Name | Purpose | Example Value / Format |
|---|---|---|
| Pipeline Run ID | Unique identifier for a specific execution instance of the Prefect extraction flow. | `run_20261011_000000` |
| Table or File Name | Identifier of the specific source batch file or domain being processed, such as stock updates, sales receipts, or expense logs. | `sales_receipts.csv` |
| Extraction Start and End Timestamps | Exact timestamps indicating when the Prefect extraction task began and finished. | `2026-10-11 00:00:00 to 2026-10-11 00:00:45` |
| Extraction Status | Final completion state of the extraction run managed by Prefect. Possible values include APPROVED, REJECTED, or MANUAL REVIEW. | `APPROVED` |
| Number of Records Read | Total count of raw rows scanned from the source CSV batch files. | `150` |
| Number of Records Extracted | Total count of valid records captured into pandas DataFrames for downstream processing. | `150` |
| Number of Records Rejected, if Applicable | Count of source records dropped or quarantined due to extraction-level validation failures. | `0` |
| Extraction Window or Data Range, if Applicable | Temporal boundary identifying the target date for daily incremental extraction, such as the previous calendar day. | `2026-10-10` |
| Validation Results | Outcome summary of the nine extraction-level checks, including source connection, file accessibility, schema matching, and foreign key verification. | `PASS` |
| Error Message or Error Code, if Applicable | Diagnostic message or system code captured by Prefect if a task fails or times out. | `NONE` |
| Extraction Duration | Total elapsed time required to complete the extraction flow under the 30-minute timeout limit. | `45 seconds` |

### Usage of Metadata and Logs
    Monitoring: Prefect operational logs and audit summaries track execution schedules, task states, and completion times in real time to ensure daily 12:00 AM runs finish within the 30-minute timeout limit. 

    Troubleshooting: Engineers examine error messages, timestamps, and failed run IDs stored in the dedicated logs directory to diagnose missing batch files, permission blocks, or schema drift. 

    Auditing:  Audit records maintain a permanent history of extracted filenames, row counts per domain, validation outcomes, and run statuses to support business reporting requirements. 
    
    Recovery: Operators use run statuses, execution windows, and quarantine logs to safely re-run failed tasks, re-extract corrected source files, and prevent duplicate data insertion.
