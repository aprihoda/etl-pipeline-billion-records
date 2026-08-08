# ETL Pipeline: ~1.2 Billion Records Processed in a 5 GB Environment

Fannie Mae Single-Family Loan Performance Data  
Anne M. Prihoda

[Live site](https://aprihoda.github.io/credit-risk-scorecard/) · [Credit risk scorecard](https://github.com/aprihoda/credit-risk-scorecard)

| | |
|---|---|
| Data sets processed | 48 |
| Total monthly records | 1,178,508,035 |
| Total loans | 19,144,733 |
| Total defaults | 519,508 |
| Total SAS processing time | 2:10:57 |
| Records per second | 149,997 |

## What the pipeline does

- Processes 48 Fannie Mae quarterly loan performance files - over one billion pipe-delimited monthly records
- Operates inside a 5 GB SAS OnDemand environment with a 1 GB per-file upload limit
- Quarters exceeding the 1 GB upload limit are pre-split on loan boundaries by a PowerShell utility: Cutting only between loans keeps loan history intact
- Source files are read directly from gzip through the FILENAME ZIP engine: Enables 35 GB of raw text to be processed within the 5 GB storage limit
- Retains only the fields the scorecard requires, applies informats to convert MMYYYY dates and coded text, and derives the modelling fields: minimum credit score, origination quarter, and the default flag
- Collapses each loan history of monthly records into a single analytical row carrying its origination characteristics and final outcome, then appends it to the master table
- Runs idempotently - reprocessing a file replaces its prior load, never duplicates it
- Records the counts for every file in the processing record, which reconciles to Fannie Mae published statistical summaries

**Full report, including the complete processing record:** [EtlPipeline_Report.pdf](EtlPipeline_Report.pdf)

## Repository contents

| File | Role |
|---|---|
| `ETLPipeline_CreditRiskScorecard.sas` | The pipeline program — libraries, macros, processing calls, checks, report |
| `SplitOversizedQuarters.ps1` | Loan-boundary split-and-gzip utility with built-in verification |
| `ImportSession.log` | The SAS session log of the processing runs |
| `EtlPipeline_Report.pdf` | Pipeline report — overview, full processing record, grand totals |
