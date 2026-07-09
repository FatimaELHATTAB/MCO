# Use `job_execution_id` End-to-End for Matching, VAT and Enrichment

## 1. Summary

Today, **Batch Collect** notifies **UM_MATCHING** with a `job_execution_id`.

However, when **UM_MATCHING** notifies downstream processes, it still uses `start_time` / `end_time`:

* **TVA** is notified with `start_time` / `end_time`
* **Enrichment** is notified with `start_time` / `end_time`

The target solution is to use `job_execution_id` everywhere, both in notifications and in the impacted data tables.

---

## 2. Current State

```mermaid
flowchart LR
    BC[Batch Collect]
    UMM[UM_MATCHING]
    TVA[TVA]
    ENR[Enrichment]

    BC -->|job_execution_id| UMM

    UMM -->|start_time / end_time| TVA
    UMM -->|start_time / end_time| ENR

    classDef source fill:#E8F1FF,stroke:#1D5FD1,color:#000;
    classDef process fill:#F2F6FF,stroke:#1D5FD1,color:#000;
    classDef vat fill:#EAF8EA,stroke:#3A8F3A,color:#000;
    classDef enrichment fill:#FFF4DC,stroke:#D99A00,color:#000;

    class BC source;
    class UMM process;
    class TVA vat;
    class ENR enrichment;
```

### Current issue

The same execution is not identified consistently across the full chain.

`job_execution_id` is used between **Batch Collect** and **UM_MATCHING**, but downstream processes still rely on time-based filtering.

This can create issues with:

* timezone handling;
* missing or duplicated records;
* unclear traceability;
* harder replay of a previous execution.

---

## 3. Target Solution

```mermaid
flowchart LR
    BC[Batch Collect]
    UMM[UM_MATCHING]
    TVA[TVA]
    ENR[Enrichment]

    BC -->|job_execution_id| UMM

    UMM -->|job_execution_id| TVA
    UMM -->|job_execution_id| ENR

    classDef source fill:#E8F1FF,stroke:#1D5FD1,color:#000;
    classDef process fill:#F2F6FF,stroke:#1D5FD1,color:#000;
    classDef vat fill:#EAF8EA,stroke:#3A8F3A,color:#000;
    classDef enrichment fill:#FFF4DC,stroke:#D99A00,color:#000;

    class BC source;
    class UMM process;
    class TVA vat;
    class ENR enrichment;
```

### Target principle

`job_execution_id` becomes the unique technical key used to identify the scope of data for one execution.

Instead of filtering records with:

```sql
WHERE updated_at >= :start_time
  AND updated_at < :end_time
```

we use:

```sql
WHERE job_execution_id = :job_execution_id
```

---

## 4. Data Propagation

The `job_execution_id` must be stored in all impacted tables produced or updated by the flow.

```mermaid
flowchart TB
    BC[Batch Collect]
    UMM[UM_MATCHING]

    subgraph TABLES[Impacted tables]
        LN[legacy_normalized]
        L[legacy]
        T[tax]
        O[Other related tables]
    end

    BC -->|job_execution_id| UMM

    UMM -->|propagate job_execution_id| LN
    UMM -->|propagate job_execution_id| L
    UMM -->|propagate job_execution_id| T
    UMM -->|propagate job_execution_id| O

    classDef process fill:#F2F6FF,stroke:#1D5FD1,color:#000;
    classDef table fill:#F7F7F7,stroke:#777,color:#000;

    class BC,UMM process;
    class LN,L,T,O table;
```

Examples of impacted tables:

* `legacy_normalized`
* `legacy`
* `tax`
* address-related tables
* identifier-related tables
* lifecycle-related tables
* any other table used by downstream processes

---

## 5. VAT Process Impact

### Current logic

TVA receives:

```text
start_time
end_time
```

Then it retrieves records modified during this time window.

### Target logic

TVA receives:

```text
job_execution_id
```

Then it retrieves the relevant TPN IDs using the execution identifier.

Example:

```sql
SELECT DISTINCT tpn_id
FROM tax
WHERE job_execution_id = :job_execution_id;
```

This avoids relying on timestamps to define the VAT batch scope.

---

## 6. Enrichment Process Impact

### Current logic

Enrichment receives:

```text
start_time
end_time
```

Then it identifies the TPN IDs to enrich based on the time window.

### Target logic

Enrichment receives:

```text
job_execution_id
```

Then it retrieves the TPN IDs linked to this execution.

Example:

```sql
SELECT DISTINCT tpn_id
FROM legacy
WHERE job_execution_id = :job_execution_id;
```

or, depending on the enrichment scope:

```sql
SELECT DISTINCT tpn_id
FROM tax
WHERE job_execution_id = :job_execution_id;
```

---

## 7. End-to-End Target Flow

```mermaid
sequenceDiagram
    participant BC as Batch Collect
    participant UMM as UM_MATCHING
    participant DB as Legacy / Tax / Related Tables
    participant TVA as TVA
    participant ENR as Enrichment

    BC->>UMM: Notify with job_execution_id
    UMM->>DB: Store / propagate job_execution_id
    UMM->>TVA: Notify with job_execution_id
    UMM->>ENR: Notify with job_execution_id

    TVA->>DB: Retrieve TPN IDs by job_execution_id
    ENR->>DB: Retrieve TPN IDs by job_execution_id
```

---

## 8. Technical Changes

| Area                        | Current                           | Target                      |
| --------------------------- | --------------------------------- | --------------------------- |
| Batch Collect → UM_MATCHING | `job_execution_id`                | No change                   |
| UM_MATCHING → TVA           | `start_time` / `end_time`         | `job_execution_id`          |
| UM_MATCHING → Enrichment    | `start_time` / `end_time`         | `job_execution_id`          |
| Data selection              | Time window                       | Execution ID                |
| Tables                      | No common execution ID everywhere | Add `job_execution_id`      |
| Replay                      | Based on time range               | Based on `job_execution_id` |

---

## 9. Main Benefits

* Same identifier used across the full chain
* No dependency on timezone-sensitive timestamps
* More reliable data scope
* Easier replay of a previous execution
* Better traceability between matching, VAT and enrichment
* Clearer link between notifications and stored data

---

## 10. Target Rule

All notifications and all impacted tables should use the same execution identifier:

```text
job_execution_id
```

The goal is to remove `start_time` / `end_time` from downstream notification logic and use `job_execution_id` as the unique scope identifier.


## 8. Technical Changes

| Area                        | Current                                                                          | Target                                                                          |
| --------------------------- | -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| Batch Collect → UM_MATCHING | `job_execution_id` is already sent                                               | No change                                                                       |
| UM_MATCHING → TVA           | Notification uses `start_time` / `end_time`                                      | Notification should use `job_execution_id`                                      |
| UM_MATCHING → Enrichment    | Notification uses `start_time` / `end_time`                                      | Notification should use `job_execution_id`                                      |
| Data selection              | Records are selected using a time window                                         | Records should be selected using `job_execution_id`                             |
| Database tables             | `job_execution_id` is not available in all impacted tables                       | Update the database table structure to add a `job_execution_id` column          |
| Data insertion              | Records are inserted without a consistent execution identifier across all tables | Populate the `job_execution_id` column during insert/update                     |
| Replay                      | Replay is based on `start_time` / `end_time`                                     | Replay should be based on `job_execution_id`                                    |
| Traceability                | Traceability depends on timestamps                                               | Traceability should rely on the same execution identifier across the full chain |

### Database structure change

The structure of the impacted database tables must be updated to include a new column:

```sql
job_execution_id
```

This column should be added to all tables used by downstream processes, for example:

* `legacy_normalized`
* `legacy`
* `tax`
* address-related tables
* identifier-related tables
* lifecycle-related tables
* any other table required by TVA or Enrichment

The objective is to make sure that all records produced or updated during the same execution can be retrieved using the same identifier.

### Insert / update logic

During data insertion or update, the `job_execution_id` must be stored with the inserted or updated records.

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

This will allow TVA and Enrichment to retrieve the impacted TPN IDs without relying on `start_time` / `end_time`.

---

## 9. Open Questions

| Question                                        | Description                                                                                                                                                                            |
| ----------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Which `job_execution_id` should be stored?      | Should we reuse the same `job_execution_id` received from Batch Collect, or should UM_MATCHING generate/use another execution identifier for its own processing?                       |
| Should the same ID be propagated everywhere?    | If the Batch Collect `job_execution_id` is reused, the same identifier will be available across Batch Collect, UM_MATCHING, TVA and Enrichment. This improves end-to-end traceability. |
| Do we need a separate UM_MATCHING execution ID? | If UM_MATCHING needs to track its own internal execution independently, we may need a dedicated identifier in addition to the Batch Collect `job_execution_id`.                        |
| Should we keep both identifiers?                | One option could be to store both: the upstream `job_execution_id` from Batch Collect and an internal UM_MATCHING execution ID, if needed for audit or monitoring.                     |
| What is the final column name?                  | Confirm whether the column should be named `job_execution_id`, `flow_execution_id`, or another standard name aligned with existing conventions.                                        |
