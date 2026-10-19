# Data Extraction Documentation

# 1. Data Source and Extraction Specification
  # 1. Source System
Name: BlockFlow: Design and Development of an Integrated Inventory, and Sales Management System for Magalin Hollow Blocks Trading
Purpose: To monitor and record the daily operation of the business in Magalin Hollow Blocks Trading, including a sales transaction, stocks updates, and operational expenses. The Owner and Staff can record daily and the data is exported as raw CSV into the entry directory, where it serves as the source input for the BlockFlow ETL pipeline which prepares it for output in analytics.

  # 2. Source Database or File
Database management system: SQLite is the pipeline storage like (blockflow.db), there is a serverless, and self-contained relational database that stores data in a local file. But, it is not the extraction source, it is the destination where extracted data is loaded into the users, inventory, expenses, and sales tables.
File format: The file is CSV for daily raw batch files (stocks updates, sales receipts, and expenses logs) in an entry directory serving as the extraction source.

  # 3. Extraction Method
Incremental: The pipeline performs a daily incremental extraction, pulling only to the most recent days batch files rather than re-extracting the entire dataset in every run. 
Extraction: The purpose of an extraction window is to determine the transaction date fields in the raw batch files, including in updated_at (last stocks updates), sales_date (sales receipts), and expenses_date (expenses logs). Each schedule extracts only in records belonging to the previous calendar day and any already processed records are protected from duplication by the INSERT/UPSERT logic used at load time.
How data will be extracted: The extracted process is on each scheduled run, the Prefect flow triggers the extraction task, which scans the entry directory for the target days raw CSV batch files. Then the Python reads these files into in memory pandas DataFrames, which passed in the Transform stage of the pipeline.
Extraction tool: The Python extraction script using pandas, csv, and json libraries, wrapped as a Prefect @task inside a Prefect @flow. The Prefect handles the scheduling, task tracking, retries and execution logging.
Extraction schedule: 12:00 AM daily with a maximum of one active run at a time and only 30 minutes execution timeout.

 # 4.Extraction Scope
Data to be extracted: The pipeline extracts the business daily operation data. There are three domains: the stock updates (current stock levels of hollow blocks), sales transactions (hollow block sales receipts), and operational expenses (raw materials, maintenance, and other costs). This pipeline matches to define the data source of daily raw CSV batch operational logs.
Files: Per batch domain there is one daily entry in the entry directory, including the stock updates, sales receipts, and expense logs as (CSV).
Target tables: There are three tables including the Stocks, Sales, and Expenses in the SQLite database. This is extracted fields follow the documented schema, included: Stocks (size, quantity, unit, unit price, updated at, recorded by), Sales (customer name, phone number, shop name, quantity, unit price, sale date, recorded by), and Expenses (category, description, amount, expense date, recorded by).
Excluded data: The user account credentials are not extracted, since the users table exists only for system login and access control, not for operational analytics.

# 5.Source Limitations and Assumptions
The data extraction process is limited by the availability and quality of the daily raw CSV files. Some source data may contain missing values, duplicate records, or inconsistent date and number formats, which may require cleaning and checking before loading the data into the database. The extraction is also limited to the available operational data, including sales, stocks, and expense records. Some historical records may be incomplete because the business previously relied on manual logs and spreadsheets. The extraction process also depends on the defined database schema and relationships. Changes in field names, data types, or required fields may require adjustments to the extraction and transformation process. External information, such as competitor pricing and market costs, is outside the scope of the extraction.
The extraction process assumes that the required daily raw CSV files are available in the designated folder and that the system has the necessary read and write permissions. It also assumes that the local SQLite database is accessible and that the required tables and relationships are already available. The process also assumes that the extracted data follows the required format and contains valid references for fields such as recorded_by and stocks_id. It is also assumed that the extracted data represents the actual operational records of Magalin Hollow Blocks Trading and is processed only for the intended BlockFlow system and reporting purposes. Only authorized business data related to the BlockFlow system is included in the extraction.

# 2. Source Tables and Column Specification
The BlockFlow ETL pipeline includes the operational data needed to process stock, sales, expenses, and user records. The source data comes from daily raw CSV operational files. The selected tables and columns are limited to the information required for the extraction, transformation, validation, and loading process. 
Name: Users
Purpose: The Users table stores the account information of the Owner/Admin and Staff who access and record data in the BlockFlow system.
Important Columns: The important columns are email (TEXT), password (TEXT), and role (TEXT). The email is used as a unique identifier, while the role identifies the user's system access.
Primary Key or Unique Identifier: The email serves as the unique identifier.
Relevant Relationships: A user can record multiple sales transactions and expense entries through the recorded_by field.
Reason for Inclusion: This table is included because user information and the recorded_by relationship are needed to identify which authorized user recorded the operational data.
Name: Stocks
Purpose: The Stocks table stores the current stock information of hollow blocks, which is needed for stock monitoring and stock updates.
Important Columns: The important columns are size (TEXT), quantity (INTEGER), unit (TEXT), unit_price (DECIMAL), updated_at (DATETIME), and recorded_by (INTEGER).
Primary Key or Unique Identifier: No explicit primary key is specified.
Relevant Relationships: The recorded_by field is a foreign key that references the Users table. The schema also states that one stock item is referenced across multiple sales orders.
Reason for Inclusion: This table is included because stock quantity and stock updates are part of the BlockFlow stock management process and are also related to sales transactions and available stock monitoring.
Name: Expenses
Purpose: The Expenses table stores the operational expenses recorded by the business, including expenses related to raw materials, maintenance, and other operating activities.
Important Columns: The important columns are category (TEXT), description (TEXT), amount (TEXT), expense_date (DECIMAL), and recorded_by (INTEGER).
Primary Key or Unique Identifier: No explicit primary key is specified.
Relevant Relationships: The recorded_by field is a foreign key that references the Users table.
Reason for Inclusion: This table is included because expense records are required for financial summaries, profit calculations, reports, and the business analytics features of BlockFlow.
Name: Sales
Purpose: The Sales table stores the daily sales transactions of Magalin Hollow Blocks Trading.
Important Columns: The important columns are customer name (TEXT), phone_number (TEXT), shop_name (TEXT), quantity (INTEGER), unit_price (DECIMAL), total_amount (DECIMAL), sale_date (DATETIME), and recorded_by (INTEGER).
Primary Key or Unique Identifier: No explicit primary key is specified. The total_amount is a derived field calculated from the quantity and unit price.
Relevant Relationships: The recorded_by field references the Users table, while the Lab 2 schema states that sales transactions reference one stock item through inventory_id.
Reason for Inclusion: This table is included because sales data is the main source for daily sales monitoring, sales analysis, forecasting, profit calculations, and AI-generated reports in BlockFlow 



# 3. Extraction Validation and Data Quality Checks
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

# 5. Log Retention and Access
The extraction flow that produces two kinds of records is operational logs generated by Prefect during each pipeline run  and audit records that document what data is extracted, when, and with what outcome. Because the pipeline processes the business sales and financial data, audit records that carry a longer retention than of operational logs. These are the three:
# Storage location
  - Operational logs: That stored locally by Prefect in its standard log storage on the pipeline PC, then have a plain-text copy written to a dedicated logs/directory and organized as one file per run.
  - Audit records: This is to be written to an audit/ directory as one summary file per run, recording the run timestamp, the files extracted, the row counts per domain, the validation check results, and the final task state.
# Access permissions
  - Owner: Both directories reside on the same standalone PC as the pipeline and blockflow.db, consistent with the system's local, self-contained design.
  - Staff: There is no access to logs. The Staff record operational data but do not manage the pipeline, so log access is restricted to prevent accidental modification or deletion of troubleshooting and audit evidence.
  - The access is enforced by operating-system file permissions on the logs/ and audit/ directories rather than inside the application, matching the standalone PC deployment.
# Archiving requirements
  - At the start of each month, the previous month's log and audit files are compressed into a single archive and the originals are moved out of the active directories. This keeps the active directories small and makes a year of audit history easy to locate.
  - Audit archives are verified as readable before the originals are removed.

# 6. Extraction Data Contract and Naming Convention
The data contract defines the exact structure every extracted batch must conform to before it enters the pipeline, and the naming rules that keep the pipeline's fields consistent from source files to the destination database. 
## 1. Required Schema

Table 1: Users

| Column Name | Data Type | Key/Constraint |
| ----------- | --------- | -------------- |
| email       | VARCHAR   | Unique         |
| password    | VARCHAR   | NOT NULL       |
| role        | VARCHAR   | NOT NULL       |

Table 2: Expenses

| Column Name  | Data Type | Key/Constraint |
| ------------ | --------- | -------------- |
| category     | VARCHAR   | NOT NULL       |
| description  | TEXT      | NOT NULL       |
| amount       | DECIMAL   | NOT NULL       |
| expense_date | DATETIME  | NOT NULL       |
| recorded_by  | INTEGER   | FOREIGN KEY    |

Table 3: Stocks

| Column Name | Data Type | Key/Constraint |
| ----------- | --------- | -------------- |
| size        | VARCHAR   | NOT NULL       |
| quantity    | INTEGER   | NOT NULL       |
| unit        | VARCHAR   | NOT NULL       |
| unit_price  | DECIMAL   | NOT NULL       |
| updated_at  | DATETIME  | NOT NULL       |
| recorded_by | INTEGER   | FOREIGN KEY    |

Table 4: Sales

| Column Name   | Data Type | Key/Constraint |
| ------------- | --------- | -------------- |
| customer_name | VARCHAR   | NOT NULL       |
| phone_number  | VARCHAR   | NOT NULL       |
| shop_name     | VARCHAR   | NOT NULL       |
| quantity      | INTEGER   | NOT NULL       |
| unit_price    | DECIMAL   | NOT NULL       |
| total_amount  | DECIMAL   | DERIVED        |
| sale_date     | DATETIME  | NOT NULL       |
| recorded_by   | INTEGER   | FOREIGN KEY    |

Required identifiers and constraints. Every extracted table carries its primary key (expenses_id, stock_id, sales_id) as a unique, non-null identifier. recorded_by is required and non-null in every record of all three tables, referencing users.user_id. stock_id is required and non-null in every Sales record, referencing stocks.stock_id. total_amount is a derived field, computed as quantity × unit_price, and is not accepted from the source file as an input value. 

# 2. Naming Convention
Column naming format: All column names use the snake_case, with lowercase letters and words separated by underscores, such as customer_name, phone_number, shop_name, unit_price, total_amount, sale_date, recorded_by, and updated_at.
Abbreviations and naming consistency: Unclear abbreviations are not permitted, need to complete and understandable names are used whenever possible. A concept keeps one name across all tables: quantity means units of hollow blocks in both the Sales and Stocks tables, and date fields follow the schema's existing convention of _date for event dates (sale_date, expense_date) and _at for last-update timestamps (updated_at).
Primary keys: The primary key of each table is named using the table name followed by _id, such as user_id, stock_id, sale_id, and expense_id.
Foreign keys: A foreign key uses the same identifier name as the referenced primary key; for example, stock_id in the Sales table references stocks.stock_id in the Stocks table. One exception applies to role-based foreign keys, which are named for the role they record rather than the referenced table: recorded_by references users.user_id and identifies the user who recorded the transaction.

# 3. Schema Consistency
Unexpected changes to column names, data types, or required fields are handled as follows. Before any batch is accepted, the required-columns check compares each file's headers against the data contract in Section 1. A missing or renamed required column, or a changed data type on a required field, REJECTS the batch and the mismatch is logged. A deviation is allowed only when a justified mapping is defined and documented in the table below; undeclared deviations are never silently coerced.

| Source Column | Standardized Column | Source Type | Target Type | Mapping Type |
| ------------- | ------------------- | ----------- | ----------- | ------------ |
| Customer Name | `customer_name`     | VARCHAR     | VARCHAR     | Rename       |
| Phone Number  | `phone_number`      | VARCHAR     | VARCHAR     | Rename       |
| Shop Name     | `shop_name`         | VARCHAR     | VARCHAR     | Rename       |
| Quantity      | `quantity`          | INTEGER     | INTEGER     | Rename       |
| Unit Price    | `unit_price`        | DECIMAL     | DECIMAL     | Rename       |
| Total Amount  | `total_amount`      | DECIMAL     | DECIMAL     | Derived      |
| Sale Date     | `sale_date`         | DATETIME    | DATETIME    | Rename       |
| Recorded by   | `recorded_by`       | INTEGER     | INTEGER     | Rename       |
| Updated At    | `updated_at`        | DATETIME    | DATETIME    | Rename       |
| Category      | `category`          | VARCHAR     | VARCHAR     | Rename       |
| Description   | `description`       | TEXT        | TEXT        | Rename       |
| Amount        | `amount`            | DECIMAL     | DECIMAL     | Rename       |
| Expense Date  | `expense_date`      | DATETIME    | DATETIME    | Rename       |

# 7. Extraction Acceptance and Handover Rules
The conditions below determine whether the extracted data may proceed to the Transformation stage. Then the  decision is based on the results of the nine validation checks defined in Section 3, evaluated by the Prefect flow at the end of each extraction run, and recorded in the run's audit summary.

Decision Rules
| Overall Condition                         | Acceptance Decision | Pipeline Action                 |
|-------------------------------------------|---------------------|---------------------------------|
| All required tables PASS                  | APPROVED            | Proceed to Transformation       |
| One or more non-critical WARNING          | MANUAL REVIEW       | Hold pipeline                   |
| One or more critical FAIL                 | REJECTED            | Stop/retry/quarantine           |

**1. Automatic Approval**

**- Validation condition:** When every check returns PASS the source directory is reachable, all three expected batch files present and readable. Also, all required columns exist, recorded_by and stock_id non-null in every record, no duplicate records, row counts match the raw files, and all three domains extracted, the tasks completed within the configured 3 retries and the 30-minute execution timeout.

**- Acceptance criteria:** All nine checks PASS in the validation summary for the run.
**- Pipeline action:** The flow marks that run APPROVED and automatically passes to the extracted DataFrames, run metadata, and validation summary to the Transformation stage.

**- Decision mode:** It is automatic, and no human approval required.

**- Responsible person:** None, only the Prefect flow decides.

**2. Manual Review**

**- Validation condition:** The pipeline is completed, but a non-critical warning is raised, for example a record count that is valid but unusually low compared to recent runs, or the minor formatting inconsistencies in non-required columns that do not violate the schema.

**- Acceptance criteria:** There no critical check has failed, but at least one check returns WARNING.

**- Pipeline action:** The pipeline holds, then extracted data is staged but not handed over to Transformation until reviewed. The Owner is notified through the run log and audit summary.

**- Decision mode:** Manual

**- Responsible person:** The Owner or a designated Staff member with the Owner's approval that can review the warning, to confirm whether the data is acceptable, and approves the handover or escalates to rejection.

**3. Rejection or Failure**

**- Validation condition:** Any critical check fails of missing or unreadable batch files, missing required columns, null identifiers, duplicate records, row-count mismatch, a missing domain, or the task remaining Failed after 3 retries or exceeding the 30-minute timeout.

**- Acceptance criteria:** One or more critical FAIL results of the dataset cannot proceed.

**- Pipeline action:** The pipeline stops the handover. Then the transient errors are retried automatically up to 3 attempts per the pipeline configuration. Then the files that still fail are quarantined, moved from the landing directory to a rejected/ subdirectory so the next run does not reprocess them, and the failure is logged with the affected filenames for manual follow-up. The run is marked REJECTED, and the extraction is re-attempted for the same window once the source problem is corrected.

**- Decision mode:** Automatic rejection at detection time of the manual decision on re-run timing.

**- Responsible person:** The Owner decides when the source issue is fixed and the re-extraction is triggered.

**4. Handover**

**- Validation condition:** The run has reached APPROVED status (automatically, or via manual review approval).

**- Acceptance criteria:**  The extraction output is complete for all three domains and its validation summary contains no unresolved FAIL.

**- Pipeline action:** The following is passed to the Transformation stage: (1) the extracted in-memory pandas DataFrames for sales, inventory (stocks), and expenses, (2) run metadata: run timestamp, source filenames and their dates, and row counts per domain, and (3) the validation summary table with the result of each check. These are also written to the audit directory as the run's permanent record.

**- Decision mode:** Automatic once approval status is reached.

**- Responsible person:** None; the Prefect flow performs the handover and logs it.

| Validation Summary       | Result                                      |
|--------------------------|---------------------------------------------|
| Stocks table validation  | PASS                                        |
| Sales table validation   | PASS                                        |
| Expenses table validation | PASS                                       |
| User table validation    | Not applicable (excluded from the extraction) |
| Overall Extraction      | APPROVED                                    |

# 8. Common Extraction Problems and Mitigation

The problems below are drawn from the documented design of the BlockFlow pipeline, its declared dependencies, Prefect orchestration configuration, and the characteristics of its source data. Each is documented with its description, possible cause, impact, and mitigation.

**1. Missing Batch File in the Entry Directory**
**- Problem description:** There one or more of the expected daily CSV batch files (stock updates, sales receipts, expense logs) is not present when the pipeline runs.

**- Possible cause:** The pipeline metadata lists raw log files present in the entry directory as a dependency. This dependency is not met because staff have not yet exported or deposited the day's files.

**- Impact on the extraction process:** The extraction cannot produce a complete dataset, so the run cannot proceed to the Transform and Load stages for the affected domain.

**- Proposed solution or mitigation:** The Prefect flow detects the missing file, retries up to the configured 3 attempts, and if the file still does not appear, it marks the run as failed and logs the missing filename so the gap can be closed with a follow-up extraction.

**2. Insufficient File Permissions or Inaccessible Database File**

**- Problem description:** The extraction task cannot read the entry directory or cannot access the blockflow.db database file.

**- Possible cause:** The pipeline metadata requires read/write local file permissions and an accessible blockflow.db file. These requirements are unmet because of changed file permissions, the database file being moved or locked, or the pipeline running under a different account.

**- Impact on the extraction process:** The flow fails at startup before any data is read, so no run output is produced for that schedule.

**- Proposed solution or mitigation:** Prefect state tracking and error catching surface the environment failure immediately with its logged cause, allowing the operator to restore the permission or file path so the scheduled run can be re-executed.

**3. Schema Changes in the Exported Files**

**- Problem description:** The batch file arrives with different column names or structures than the documented schema.

**- Possible cause:**  A change in how the source files are generated or exported that was not reflected in the pipeline metadata.

**- Impact on the extraction process:**  Downstream processing may fail or map columns incorrectly, corrupting the meaning of the stored data.

**- Proposed solution or mitigation:**  The pipeline's validation step checks schema and foreign keys (recorded_by,stocks_id) before data is accepted. A structural mismatch stops the batch, and the schema documentation is updated in a controlled way before re-running.

**4. Erroneous Values from Manual Data Entry**

**- Problem description:** The raw records contain human input errors such as misspelled values, inconsistent formats, or blank fields.

**- Possible cause:** The source data originates from manual record-keeping, which the study's background identifies as error-prone due to human input.

**- Impact on the extraction process:** Passing erroneous values downstream would produce unreliable stock counts, sales totals, and forecasts.

**- Proposed solution or mitigation:** The system applies real-time validation and error prompts during data entry so mistakes are corrected before saving. Any errors that still reach the pipeline are handled by the Transform stage's cleaning step (dropping duplicates, filling or removing nulls, standardizing formats), which is deliberately structured into the data pipeline workflow.

**5. Duplicate Records in the Extracted Data**
**- Problem description:** The same transaction appears more than once in the extracted data.

**- Possible cause:** Repeated extraction of the same batch, since the pipeline may process a file more than once after a failed or re-run execution.

**- Impact on the extraction process:** Duplicates would inflate sales totals and stock movements in the destination tables.

**- Proposed solution or mitigation:** The Load stage executes INSERT/UPSERT transactions, so re-processing the same records updates rather than duplicates them, while the Transform stage additionally drops duplicate records.

**6. Records with Missing Foreign Key Values**
**- Problem description:** Extracted sales records lack an stocks reference or records lack a recorded_by user link.

**- Possible cause:** The source entry or export omitted the field despite entry-time validation prompts.

**- Impact on the extraction process:** Records without these foreign keys cannot satisfy the relational constraints validated by the pipeline and would break the links to users and inventory items.

**- Proposed solution or mitigation:** The pipeline's validation of schema and foreign keys rejects affected records before loading, ensuring entries are corrected at the source and re-extracted.

**7. Extraction Interruption or Timeout**

**- Problem description:** The extraction flow stops mid-run or exceeds its execution limit.

**- Possible cause:** Large or slow file reads and environmental interruptions during execution.

**- Impact on the extraction process:** A partially completed run could leave the pipeline without a complete dataset while falsely appearing to have produced output.

**- Proposed solution or mitigation:** Prefect handles error catching and automated retries (configured at 3 attempts) with an execution timeout of 30 minutes. Task states are tracked and logged so an incomplete run is visible and is never treated as a successful extraction.

**8. Contended Writes to the SQLite Database**

**- Problem description:** Simultaneous write operations compete while the pipeline loads extracted data.

**- Possible cause:** SQLite is not designed for applications with heavy simultaneous write operations, as documented in the technology trade-offs.

**- Impact on the extraction process:** Concurrent writes could slow the Load phase or cause lock contention during insertion.

**- Proposed solution or mitigation:** The pipeline configuration limits execution to a maximum of one active run (Max Active Runs: 1), and the daily 12:00 AM schedule processes batches sequentially rather than continuously, minimizing write contention by design.
