# ASE-ETL — Phase 9 Power BI Visualization & Business Intelligence

**Implementation & Validation Report**  
**Project:** ASE ETL — ETL to Insights  
**Reporting snapshot:** `20261018T114042Z_4ed531be`  
**Status:** Three report pages implemented; final presentation checks noted below  
**Basis:** Assignment PDF, user-provided Phase 6 report schemas/aggregates, final dashboard screenshots, user-confirmed validation.

> Evidence convention: “verified” denotes reported numerical results or user-confirmed actions; “recommended” denotes a change not yet explicitly confirmed.

## Executive summary

Phase 9 implements the visualization component of the “ETL to Insights” assignment using Power BI Desktop. Three report pages—Workforce Overview; Retention & Workforce Stability; and Attendance & Workload—consume the SQL-generated analytics reports from Phase 6. The report emphasizes reproducible SQL calculations, filter-aware presentation, and clear disclosure of data coverage limitations. The user confirmed month, department and employee filtering works and that Power Query loading errors were resolved. The supplied evidence establishes functional dashboard implementation, but does not independently prove a byte-for-byte reconciliation between every imported Power BI row and its source CSV.

## Assignment requirements and scope

The assignment requires at least three clear, actionable visualizations with suitable labels, formatting and interactive controls where appropriate. It also specifies nine analytics KPIs. The solution uses three Power BI report pages containing multiple visualizations and uses the Phase 6 SQL-generated reports for the business metrics. The assignment does not require cloud publication or a Power BI Service dashboard. This phase does not replace, duplicate, or revise the Bronze/Silver/Gold pipeline or the Phase 6 SQL logic.

## Source architecture and refresh design

Pipeline: local files or MinIO → Bronze (raw ingestion) → Silver (cleaning and deduplication) → Gold (dimensional tables) → SQL analytics/data marts → timestamped CSV report snapshot → Power BI Desktop Import model. The source snapshot used for implementation is D:\ASE-ETL\reports\analytics\20261018T114042Z_4ed531be. CSV files are immutable run-scoped outputs; the current PBIX points to this snapshot and will NOT automatically switch to newer run folders on refresh. A future enhancement is a stable approved-publication path or a direct PostgreSQL analytical connection. Do not combine historical report-generation folders, as that would duplicate metrics.

## Power BI semantic model

Imported Page 1 tables: 01_monthly_headcount_turnover, 08_monthly_workload, and 13_workforce_changes. A calculated DimMonth table is built from their distinct month_start values, with MonthStart, MonthLabel and MonthSort fields. One-direction, active relationships link DimMonth[MonthStart] to each report’s month_start. A dedicated _Measures table organizes DAX measures; it is not an independent store of KPI results. Page 2 adds 02_department_tenure and 07_early_attrition, which remain separate because neither supplies a monthly grain. Page 3 uses 03_employee_working_hours, 05_daily_overtime, 06_rolling_working_hours, 10_attendance_compliance and 11_department_overtime. Some of these are separate preaggregated datasets, not interchangeable star-schema fact tables. Never apply month/department/employee filtering where a source does not have the necessary dimensional grain.

## Phase 6 analytics report inventory

| Report (CSV) | Source rows |
|---|---:|
| `01_monthly_headcount_turnover.csv` | 7 |
| `02_department_tenure.csv` | 38 |
| `03_employee_working_hours.csv` | 10,773 |
| `04_daily_attendance.csv` | 379,480 |
| `05_daily_overtime.csv` | 379,480 |
| `06_rolling_working_hours.csv` | 379,480 |
| `07_early_attrition.csv` | 1 |
| `08_monthly_workload.csv` | 7 |
| `09_department_workload.csv` | 512 |
| `10_attendance_compliance.csv` | 168 |
| `11_department_overtime.csv` | 512 |
| `12_weekday_workload.csv` | 7 |
| `13_workforce_changes.csv` | 7 |

## Page 1 — Workforce Overview

Audience: an executive seeking HR headcount trends and operational timesheet coverage. Four cards display latest-selected-month HR headcount, latest-selected-month employees with timesheet records, total terminations over the selected period and total eligible worked hours over the selected period. Charts: headcount evolution; monthly hires versus terminations; monthly worked-hours volume. Shared month slicer uses DimMonth. The display must distinguish HR employee-master coverage from the much larger timesheet population. Snapshot reported in the reviewed screenshot: latest HR headcount 44; latest-month timesheet employees 6,783; period terminations 2; period worked hours approximately 2.53 million. Those figures refer to different grains and populations, so they are not directly comparable.

## Page 2 — Retention & Workforce Stability

Audience: HR analyst investigating tenure and exits. Three KPI cards present early attrition (0.00%, using SQL percentage points), average employee tenure (24.09 years in the unfiltered screenshot, a weighted combination of department means), and two employee exits across the displayed months. Charts: departmental average tenure in years (top departments), monthly employee exits, and monthly turnover rate. Turnover spikes in May 2025 (2.20%) and June 2025 (2.25%) and reads 0% in the reported April, July and August subset. Department filtering affects the tenure summary only; cohort-level attrition and time-series exits must not be falsely attributed to a department. Many departments have small employee counts: expose employee_count in tooltips and avoid equating a single-person departmental average with a larger team. April 2025 is flagged as an incomplete calendar month in the source report.

## Page 3 — Attendance & Workload

Audience: operations analyst examining punctuality, attendance eligibility, overtime and employee workload. Four KPI cards display 16,386 late arrivals; 36,692 early departures; 41,668 overtime events; and 9.29 eligible worked hours per eligible workday. Charts: daily attendance compliance; overtime frequency by department; and individual daily worked hours versus SQL-calculated seven-day rolling average. Select one employee for the rolling-hours chart: the rolling metric is employee-specific and must not be summed across employees. The employee selector does not control organization-wide cards. The overtime metric is described as SQL-derived proxy hours where used, not verified payroll overtime. The reviewed Page 3 screenshot still annotated a minimum of five eligible overtime events; a minimum of 30 was recommended but the user has not confirmed applying it. Consequently the current screenshot’s displayed 100% departments must be interpreted with their small sample sizes.

## Mandatory KPI coverage

| Assignment KPI | Page | Visual / caveat |
|---|---|---|
| Active headcount over time | Page 1 | Monthly headcount line |
| Turnover trend | Page 2 | Monthly turnover percentage line |
| Average tenure by department | Page 2 | Top-department horizontal bars |
| Average working hours per employee | Page 3 | Employee-specific rolling-hours view; explicit employee average summary recommended |
| Late arrival frequency | Page 3 | Event KPI and attendance summary |
| Early departure count | Page 3 | Event KPI and attendance summary |
| Total overtime count | Page 3 | Overtime KPI and department comparison |
| Rolling average working hours | Page 3 | Employee-selected seven-day line |
| Early attrition rate | Page 2 | Cohort KPI card |

## Confirmed data-coverage findings

Attendance: July 2025 has 9,132 eligible events, 5,562 compliant (60.91%); August 2025 has zero eligible events and therefore no meaningful compliance rate; September 2025 has 28,560 eligible events, 19,944 compliant (69.83%). A long flat 100% line between sparse eligible dates is a visualization artifact that can be misleading. The report was adjusted to draw separate continuous-date line segments around the documented gap. The actual daily source has July 9 with 6/6 eligible/compliant events, no eligible events from July 10–August 20 in the shown rows, and sparse eligible dates in September before data volume resumed. September 25 was reported at 3,339/3,339 (100%) and should not be discarded automatically. Overtime: departments with 100% frequency often had only 1–8 eligible events; one department with 76 eligible events had 74 overtime events (97.37%). A visual-level eligibility threshold is a presentation choice, not a change to SQL results. HR master is a 50-record cohort, whereas monthly timesheet reports can have thousands of distinct IDs; this prevents organization-wide claims based on HR and timesheet metrics as though they covered the same population.

## Quality assurance and validation

The user confirmed Power Query conversion problems on 05_daily_overtime and 06_rolling_working_hours were corrected and Close & Apply subsequently completed without errors. The user confirmed the month, department and employee interactions function as intended. Independent PowerShell CSV aggregation produced 16,386 late arrivals, 36,692 early departures, 41,668 overtime events, and 9.29 average hours per eligible workday; the screenshot showed exact KPI card values consistent with these figures. Historical report row counts were obtained by Import-Csv; these are source counts, not a standalone audit of Power BI imported row counts. The dashboard's screenshot evidence and user-performed checks substantiate implementation, while a formal automated PBIX-to-source reconciliation was not performed.

## DAX measure design and trade-offs

The actual business definitions—eligibility rules, late/early grace handling, overtime proxy, early attrition, and seven-day rolling logic—are implemented upstream in SQL. DAX primarily selects or aggregates precomputed results. HR Headcount returns closing_headcount for the latest selected month rather than summing snapshots; Timesheet Employees likewise returns latest-month employees_with_records and is not a distinct count across multiple months. Total Terminations and Total Worked Hours sum their monthly source values. Employee tenure uses the employee-count-weighted mean of rounded departmental average_tenure_years, so it is approximate and assumes departments are non-overlapping. Average Daily Worked Hours equals SUM(total_worked_hours)/SUM(eligible_workdays); it is workday-weighted, not the unweighted mean of per-employee averages. Attendance Compliance Rate equals SUM(compliant_events)/SUM(eligible_events) * 100 when eligible > 0, otherwise BLANK. Rates stored as percentage points (e.g., 2.20) are shown using custom numeric formatting, not Power BI's multiplying Percentage formatter. Rolling-hours visual averages are meaningful only after selecting an employee and confirming record grain.

## Visual styling and interaction

The chosen theme uses warm off-white #F6F7F2, deep ink #142A2D, teal #247D74, coral #E87869, and white chart/card surfaces. Page headers, white metric cards, modest rounded corners, restrained grids and axes, and compact dropdowns make the three pages visually coherent. Date/month, department and employee selectors are deliberately scoped to tables that support their grain. Categorical attendance-axis exploration was rejected because it caused clutter, weekday labels and horizontal scrolling; the final approach retains the continuous timeline while separating line series across the missing-eligibility interval. Other notes: remove technical field labels, display event counts without K rounding, and disclose the scope of headcount versus timesheets.

## Reproduction and usage

Prerequisites: Windows Power BI Desktop, Phase 6 report generation, access to the selected report snapshot, and an existing PBIX (ASE_Workforce_Intelligence.pbix in D:\ASE-ETL\powerbi\, per the project’s planned location). From the project, run the orchestrated pipeline or Phase 6 reporting step to produce a complete snapshot; choose one successful folder. In Power BI, verify each Text/CSV source path in Power Query, check key types (IDs should generally remain text), refresh queries and confirm no conversion errors. Open each report page and test its scoped slicer(s). Keep data labels consistent with source units: percentages in percentage points, tenure in years, and overtime proxy explicitly described. If a new snapshot folder is generated, update the source paths to the new validated snapshot (or implement a stable publication abstraction) before refreshing; pressing Refresh on the original PBIX alone does not select a new folder. Save the PBIX under powerbi/ and capture report screenshots for submission.

## Open items and sign-off criteria

Remaining release checks: (1) verify the overtime departmental visual has the intended eligibility threshold and a matching annotation; the last shared screenshot still stated five, not the recommended 30. (2) Confirm the average-tenure card clearly communicates years; its last screenshot showed 24.09 without a visible unit. (3) Optionally add a directly visible employee-level mean-hours summary from 03_employee_working_hours for the assignment's “per employee” wording; the current selected-employee rolling chart gives a per-employee perspective but is not a direct mean-hours leaderboard. (4) Confirm final PBIX is saved, and optionally reconcile Power BI table row counts to source CSV counts. The documentation records these as pending or recommended, not as completed checks.

## Dashboard screenshots (supplied during implementation)

**Page 1 — Workforce Overview**

![Page 1](Screenshot%202026-10-19%20222546.png)

**Page 2 — Retention & Workforce Stability**

![Page 2](Screenshot%202026-10-19%20222537.png)

**Page 3 — Attendance & Workload**

![Page 3](Screenshot%202026-10-19%20222529.png)

## Reference

* `Data_Engineering_Assignment_1.pdf`, especially pp. 1–5: required KPIs, minimum visualizations, interaction, engineering decisions and documentation.
