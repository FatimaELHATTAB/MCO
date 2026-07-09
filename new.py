# Use `job_execution_id` End-to-End

## 1. Context

Today, **Batch Collect** notifies **UM_MATCHING** with a `job_execution_id`.

However, **UM_MATCHING** still notifies downstream processes using `start_time` / `end_time`:

* TVA receives `start_time` / `end_time`
* Enrichment receives `start_time` / `end_time`

The target is to replace these time windows with `job_execution_id`, so the same execution identifier is used across the full chain.

---

## 2. Current vs Target Flow

```mermaid
flowchart LR
    BC[Batch Collect]
    UMM[UM_MATCHING]
    TVA[TVA]
    ENR[Enrichment]

    BC -->|job_execution_id| UMM

    UMM -->|❌ start_time / end_time<br/>✅ job_execution_id| TVA
    UMM -->|❌ start_time / end_time<br/>✅ job_execution_id| ENR

    subgraph DB[Impacted database tables]
        LN[legacy_normalized]
        L[legacy]
        T[tax]
        O[Other related tables]
    end

    UMM -.->|store job_execution_id| LN
    UMM -.->|store job_execution_id| L
    UMM -.->|store job_execution_id| T
    UMM -.->|store job_execution_id| O
```

---

## 3. Target Principle

The processing scope should no longer be based on timestamps.

Current filtering:

```sql
WHERE updated_at >= :start_time
  AND updated_at < :end_time
```

Target filtering:

```sql
WHERE job_execution_id = :job_execution_id
```

This avoids timezone issues and gives a clearer scope for replay and traceability.

---

## 4. Impacted Processes

| Process                     | Current                         | Target                      |
| --------------------------- | ------------------------------- | --------------------------- |
| Batch Collect → UM_MATCHING | Sends `job_execution_id`        | No change                   |
| UM_MATCHING → TVA           | Sends `start_time` / `end_time` | Send `job_execution_id`     |
| UM_MATCHING → Enrichment    | Sends `start_time` / `end_time` | Send `job_execution_id`     |
| TVA data selection          | Based on time window            | Based on `job_execution_id` |
| Enrichment data selection   | Based on time window            | Based on `job_execution_id` |

---

## 5. Database Changes

The impacted database tables must be updated to add a new column:

```sql
job_execution_id
```

Examples of impacted tables:

* `legacy_normalized`
* `legacy`
* `tax`
* other related tables used by TVA or Enrichment

When records are inserted or updated, the `job_execution_id` must be stored with the data.

Example:

```sql
INSERT INTO tax (
    tpn_id,
    tax_label,
    tax_value,
    job_execution_id
)
VALUES (
    :tpn_id,
    :tax_label,
    :tax_value,
    :job_execution_id
);
```

---

## 6. Example Target Queries

### TVA

```sql
SELECT DISTINCT tpn_id
FROM tax
WHERE job_execution_id = :job_execution_id;
```

### Enrichment

```sql
SELECT DISTINCT tpn_id
FROM legacy
WHERE job_execution_id = :job_execution_id;
```

The exact source table can be adjusted depending on the enrichment scope.

---

## 7. Technical Changes

| Area                 | Change                                                                           |
| -------------------- | -------------------------------------------------------------------------------- |
| Notification payload | Replace `start_time` / `end_time` with `job_execution_id` for TVA and Enrichment |
| Database structure   | Add `job_execution_id` column in impacted tables                                 |
| Insert/update logic  | Populate `job_execution_id` during data insertion or update                      |
| Data retrieval       | Retrieve impacted TPN IDs using `job_execution_id`                               |
| Replay               | Replay a previous execution using `job_execution_id` instead of a time range     |
| Logs / monitoring    | Add `job_execution_id` in logs for traceability                                  |

---

## 8. Open Questions

| Question                                         | Comment                                                                                                                 |
| ------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------- |
| Which `job_execution_id` should be stored?       | Should we reuse the `job_execution_id` received from Batch Collect?                                                     |
| Do we need another execution ID for UM_MATCHING? | UM_MATCHING may need its own internal ID for audit or monitoring.                                                       |
| Should we store one ID or two IDs?               | Option 1: store only the Batch Collect `job_execution_id`. Option 2: store both upstream and UM_MATCHING execution IDs. |
| What is the final column name?                   | Confirm whether the column name should be `job_execution_id` or another standard name.                                  |
