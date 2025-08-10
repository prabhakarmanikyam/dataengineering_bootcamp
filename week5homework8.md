# Pipeline Ownership & Governance Plan

## Business Areas and Pipelines

### Profit
1. **Unit-Level Profit for Experiments**
   - Primary Owner: Data Engineer A
   - Secondary Owner: Data Engineer B
2. **Aggregate Profit (Reported to Investors)**
   - Primary Owner: Data Engineer B
   - Secondary Owner: Data Engineer A

### Growth
3. **Aggregate Growth (Reported to Investors)**
   - Primary Owner: Data Engineer C
   - Secondary Owner: Data Engineer D
4. **Daily Growth for Experiments**
   - Primary Owner: Data Engineer D
   - Secondary Owner: Data Engineer C

### Engagement
5. **Aggregate Engagement (Reported to Investors)**
   - Primary Owner: Data Engineer A
   - Secondary Owner: Data Engineer D

---

## On-Call Schedule

| Week        | On-Call Engineer | Backup Engineer |
|-------------|------------------|-----------------|
| Week 1      | A                | B               |
| Week 2      | B                | C               |
| Week 3      | C                | D               |
| Week 4      | D                | A               |
| Week 5+     | Repeat rotation  |                 |

**Rules:**
- Holidays: If an engineer is on holiday, the next in rotation swaps weeks.
- On-call covers 24/7 support but primary responds first, backup steps in if unavailable.
- Each engineer gets 1 week on-call, 3 weeks off.

---

## Run Books for Investor-Reporting Pipelines

### Profit – Aggregate Profit
- **Data Sources**: Transaction DB, Cost Allocation Tables
- **Transformations**: Aggregation by product, region, and time period
- **Checks**:
  - Data completeness per fiscal month
  - Cost/profit margin thresholds
- **Outputs**: Monthly CSV + dashboard feed to investor portal
- **Recovery Steps**:
  - Check job logs for extraction failures
  - Re-run transformation queries
  - Validate against source DB counts

### Growth – Aggregate Growth
- **Data Sources**: User Registration DB, Activity Logs
- **Transformations**: Net new users calculation, churn adjustments
- **Checks**:
  - Daily registration counts match event logs
  - Churn metrics align with subscription cancellations
- **Outputs**: Weekly growth report + dashboard
- **Recovery Steps**:
  - Identify missing days in data warehouse
  - Backfill using historical logs

### Engagement – Aggregate Engagement
- **Data Sources**: App Event Stream, Session Logs
- **Transformations**: Monthly active users (MAU), daily active users (DAU)
- **Checks**:
  - No spikes/drops >30% without annotation
  - Session duration > 0 seconds
- **Outputs**: Monthly engagement report + investor dashboard
- **Recovery Steps**:
  - Validate event ingestion pipeline
  - Fill missing event data from backup storage

---

## Potential Failure Points

- **Data Source Outage**
  - Transaction DB, Event Streams, or Logs unavailable
- **ETL Job Failures**
  - Scheduler crashes, transformation errors, schema changes
- **Late Arriving Data**
  - Delays in upstream pipelines affecting reporting
- **Schema Drift**
  - Source systems adding/removing columns without notice
- **Data Quality Issues**
  - Null values in critical fields, incorrect mappings
- **Storage/Partition Issues**
  - Warehouse storage limits reached, partition misalignment
- **Report Publishing Failures**
  - API errors when pushing data to dashboards

---

## Notes
- This plan assumes 4 engineers sharing ownership evenly.
- All investor-reporting pipelines require stricter SLAs (e.g., <4h resolution for failures).
- Run books should be stored in internal Confluence and mirrored in Git for version control.

