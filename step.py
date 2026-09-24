# External Provider Delta Management

## 1. Purpose

This page describes the target behaviour for processing delta updates received from external providers.

The objective is to ensure that provider updates are correctly propagated across the matching ecosystem while preserving data history and maintaining consistency between:

* provider records,
* matching results,
* clusters,
* prioritised identification values,
* Tax / VAT information,
* and any other downstream table whose selected value depends on matching and source prioritisation.

Two types of provider updates must be distinguished:

| Update type             | Description                                                                                                                  | Matching replay | Cluster impact  |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------- | --------------- | --------------- |
| `update_identification` | One or more identification attributes change without invalidating the existing matching relationship.                        | No              | No              |
| `update_matching`       | One or more attributes used by the matching algorithm change. The previous matching result may therefore no longer be valid. | Yes             | Potentially yes |

---

# 2. High-Level Processing

```mermaid
flowchart TD

    A[External provider delta received] --> B{Update type?}

    B -->|update_identification| C{Is the local_id already matched?}

    C -->|No| D[No action]
    C -->|Yes| E[Retrieve associated TPN ID]

    E --> F[Re-run impacted prioritisation]
    F --> G[Update downstream values if required]

    B -->|update_matching| H{Is the local_id already matched?}

    H -->|No| I[Run standard matching chain]

    H -->|Yes| J[Retrieve previous state]
    J --> K[Close previous matching]
    K --> L[Build baseline without previous contribution]
    L --> M[Replay updated record through standard matching chain]
```

---

# 3. `update_identification`

## 3.1 Principle

An `update_identification` does not invalidate the existing matching relationship.

The matching algorithm must therefore **not** be replayed.

The existing association between the provider `local_id` and the `TPN_ID` is preserved.

The cluster is also preserved.

Only the downstream information potentially impacted by the updated identification values must be re-evaluated.

> **`update_identification` keeps the existing matching and cluster and only replays the relevant prioritisation logic.**

---

# 4. `update_identification` Processing

When an `update_identification` is received, the first step is to determine whether the provider `local_id` is already matched.

## Case 1 — The `local_id` is not matched

No further action is required.

The identification update must not trigger a new matching attempt.

---

## Case 2 — The `local_id` is already matched

The corresponding `TPN_ID` is retrieved.

The updated provider values are then used to replay the prioritisation logic for the impacted attributes.

Depending on the updated information, this may include:

* Tax / VAT;
* identification information;
* legal identifiers;
* equality / identity-related values;
* other TPN-level attributes derived through source prioritisation.

For each impacted attribute, the newly prioritised value is compared with the currently selected value.

If the result does not change, no update is required.

If the result changes, the previous value is historised and the newly prioritised value becomes the current one.

---

# 5. `update_identification` Decision Tree

```mermaid
flowchart TD

    A[update_identification received] --> B{Is the local_id already matched?}

    B -->|No| C[Stop processing]

    B -->|Yes| D[Retrieve TPN ID]

    D --> E[Identify impacted attributes]

    E --> F[Re-run source prioritisation]

    F --> G{Selected value changed?}

    G -->|No| H[Keep current value]

    G -->|Yes| I[Close previous value]

    I --> J[Insert newly selected value]

    J --> K{Other impacted attributes?}

    K -->|Yes| F
    K -->|No| L[Processing completed]
```

---

# 6. `update_identification` Example

Assume the current matching is:

| Provider | local_id | TPN_ID | closed_at |
| -------- | -------- | -----: | --------- |
| Orbis    | ORB_001  | 123456 | `NULL`    |

And the current Tax value is:

| TPN_ID | VAT       | Selection        | closed_at |
| -----: | --------- | ---------------- | --------- |
| 123456 | VAT_ORBIS | `matching_orbis` | `NULL`    |

An `update_identification` is received for `ORB_001`.

The matching remains unchanged.

After replaying Tax prioritisation, assume Refinitiv should now be selected.

### Result

| TPN_ID | VAT           | Selection            | closed_at             |
| -----: | ------------- | -------------------- | --------------------- |
| 123456 | VAT_ORBIS     | `matching_orbis`     | `2026-09-24 10:30:00` |
| 123456 | VAT_REFINITIV | `matching_refinitiv` | `NULL`                |

The important point is that:

* `ORB_001` remains matched to the same `TPN_ID`;
* the matching algorithm is not replayed;
* the cluster is not recalculated;
* only the impacted prioritisation has changed.

---

# 7. `update_identification` Sequence Diagram

```mermaid
sequenceDiagram

    participant P as External Provider
    participant D as Delta Processing
    participant MT as Matching Table
    participant PR as Prioritisation
    participant T as Downstream Tables

    P->>D: update_identification

    D->>MT: Search matching for local_id

    alt local_id is not matched
        MT-->>D: No matching found
        D-->>D: Stop processing

    else local_id is matched
        MT-->>D: Return TPN_ID

        D->>PR: Replay impacted prioritisation
        PR->>T: Retrieve current selected values

        alt Selected value unchanged
            PR-->>T: No change

        else Selected value changed
            PR->>T: Close previous value
            PR->>T: Insert newly selected value
        end
    end
```

---

# 8. `update_matching`

## 8.1 Principle

An `update_matching` means that at least one attribute used by the matching process has changed.

The previous matching result can therefore no longer automatically be considered valid.

The processing depends on whether the provider `local_id` is already matched.

| Situation                               | Behaviour                                                                                                                                                 |
| --------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| The `local_id` is not currently matched | Run the standard matching chain normally.                                                                                                                 |
| The `local_id` is currently matched     | Remove the impact of the previous matching, establish a consistent baseline, then replay the updated record through the complete standard matching chain. |

---

# 9. `update_matching` Without Existing Matching

If the provider `local_id` is not currently matched, there is no previous matching state to undo.

The updated provider record simply follows the standard process:

```text
Updated provider record
        ↓
Standard matching
        ↓
Cluster calculation
        ↓
Prioritisation
        ↓
Tax / Identification
        ↓
Final state
```

No delta-specific rollback is required.

---

# 10. `update_matching` With Existing Matching

This is the main delta-specific scenario.

The processing must be divided into two clearly separated phases.

### Phase 1 — Build the baseline

Remove the impact of the previous matching and reconstruct the valid state without the old provider contribution.

### Phase 2 — Replay the updated provider

Process the updated provider record through the complete standard matching chain.

The baseline is therefore an **intermediate state**, not the final result.

---

# 11. Phase 1 — Retrieve the Previous State

Before modifying the current state, the system must retrieve the information associated with the provider `local_id`.

This includes, where applicable:

* the associated `TPN_ID`;
* the existing matching row;
* the current cluster information;
* impacted `cluster_stat` rows;
* downstream values currently selected through prioritisation;
* values whose selection depends on the provider being updated.

This information is required both to remove the impact of the previous matching and to compare the state before and after the replay.

---

# 12. Phase 1 — Close the Previous Matching

Example:

### Before

| Provider | local_id | TPN_ID | closed_at |
| -------- | -------- | -----: | --------- |
| Orbis    | ORB_001  | 123456 | `NULL`    |

### After

| Provider | local_id | TPN_ID | closed_at             |
| -------- | -------- | -----: | --------------------- |
| Orbis    | ORB_001  | 123456 | `2026-09-24 10:30:00` |

The previous matching contribution must no longer be taken into account when calculating the baseline.

---

# 13. Phase 1 — Build the Baseline

After removing the previous matching contribution, prioritisation is replayed.

The objective is to answer:

> **What should the TPN-level state be if the previous matching of this provider no longer existed?**

Example:

```text
TPN_ID 123456

Orbis
 └── VAT_ORBIS_OLD

Refinitiv
 └── VAT_REFINITIV
```

Assume the current selected Tax value is:

| TPN_ID | VAT           | Selection        | closed_at |
| -----: | ------------- | ---------------- | --------- |
| 123456 | VAT_ORBIS_OLD | `matching_orbis` | `NULL`    |

After removing the old Orbis matching contribution, Refinitiv becomes the selected source.

The resulting baseline is:

| TPN_ID | VAT           | Selection            | closed_at             |
| -----: | ------------- | -------------------- | --------------------- |
| 123456 | VAT_ORBIS_OLD | `matching_orbis`     | `2026-09-24 10:31:00` |
| 123456 | VAT_REFINITIV | `matching_refinitiv` | `NULL`                |

This represents the valid state before the updated Orbis record is replayed.

---

# 14. The Baseline Is Not the Final Result

This distinction is critical.

The baseline only represents the state obtained after removing the old provider matching contribution.

The updated provider has not yet been processed.

Once the baseline is available, the updated record goes through the complete standard matching chain.

The final value may therefore change again.

Example:

```text
Initial state

VAT_ORBIS_OLD
      ↓
Remove old Orbis contribution
      ↓
Baseline

VAT_REFINITIV
      ↓
Replay updated Orbis through standard chain
      ↓
Final state

VAT_ORBIS_NEW
```

A single `update_matching` may therefore produce several successive historical values.

---

# 15. Phase 2 — Replay the Standard Matching Chain

Once the baseline has been established, the delta-specific part of the process is complete.

The updated provider record must now go through the **regular end-to-end matching chain**.

```text
Updated Provider Record

        ↓

Standard Matching Algorithm

        ↓

Matching Result

        ↓

Cluster Calculation

        ↓

Cluster Reconciliation

        ↓

Standard Prioritisation

        ↓

Tax / Identification / Other Downstream Processing

        ↓

Final State
```

From this point onward, the normal matching behaviour applies.

The delta process must not introduce a specific shortcut or alternative matching logic.

---

# 16. Complete `update_matching` Decision Tree

```mermaid
flowchart TD

    A[update_matching received] --> B{Is the local_id already matched?}

    B -->|No| C[Run standard matching chain]

    C --> D[Standard matching]
    D --> E[Cluster calculation]
    E --> F[Standard prioritisation]
    F --> G[Tax / Identification processing]

    B -->|Yes| H[Retrieve previous matching and cluster state]

    H --> I[Close previous matching]

    I --> J[Build baseline]

    J --> K[Replay prioritisation without old provider contribution]

    K --> L[Update impacted downstream values if required]

    L --> M[Baseline reached]

    M --> N[Submit updated provider to standard chain]

    N --> O[Standard matching]

    O --> P[Standard cluster calculation]

    P --> Q[Compare previous and new cluster states]

    Q --> R[Reconcile impacted clusters]

    R --> S[Standard prioritisation]

    S --> T[Tax / Identification processing]

    T --> U[Final consistent state]
```

---

# 17. Cluster Reconciliation

Before replaying the provider update, the previous cluster state must be retained.

After standard matching is executed, the cluster calculation is replayed and the resulting state is compared with the previous one.

Example:

### Previous State

```text
Cluster C001

TPN 123456
TPN 456789
```

### New State

```text
Cluster C002

TPN 123456
TPN 987654
```

If the cluster changes, the previous cluster state must be historised and the newly calculated cluster state inserted.

### Example

| Cluster | TPN_ID | closed_at             |
| ------- | -----: | --------------------- |
| C001    | 123456 | `2026-09-24 10:35:00` |
| C001    | 456789 | `2026-09-24 10:35:00` |
| C002    | 123456 | `NULL`                |
| C002    | 987654 | `NULL`                |

The objective is to avoid keeping obsolete and newly calculated cluster representations simultaneously as the current state.

---

# 18. Cluster Decision Tree

```mermaid
flowchart TD

    A[Standard matching replay completed] --> B[Calculate resulting cluster]

    B --> C[Retrieve previous cluster state]

    C --> D{Cluster state changed?}

    D -->|No| E[Keep current consistent state]

    D -->|Yes| F[Identify obsolete cluster rows]

    F --> G[Close obsolete cluster rows]

    G --> H[Insert newly calculated cluster state]

    H --> I[Continue standard prioritisation]
```

---

# 19. Rich End-to-End Example

Assume the provider record is:

```text
Provider = Orbis
local_id = ORB_001
```

It is currently matched to:

```text
TPN_ID = 123456
```

### Initial Matching

| Provider | local_id | TPN_ID | closed_at |
| -------- | -------- | -----: | --------- |
| Orbis    | ORB_001  | 123456 | `NULL`    |

### Available Tax Candidates

| Source    | VAT candidate   |
| --------- | --------------- |
| Orbis     | `VAT_ORBIS_OLD` |
| Refinitiv | `VAT_REFINITIV` |

The current selection is:

| TPN_ID | VAT             | Selection        | closed_at |
| -----: | --------------- | ---------------- | --------- |
| 123456 | `VAT_ORBIS_OLD` | `matching_orbis` | `NULL`    |

An `update_matching` is received for `ORB_001`.

Assume the provider update contains:

| Attribute           | Before        | After         |
| ------------------- | ------------- | ------------- |
| Registration Number | REG_001       | REG_999       |
| VAT Number          | VAT_ORBIS_OLD | VAT_ORBIS_NEW |

---

# 20. Step 1 — Remove the Previous Matching Contribution

The existing matching is closed.

| Provider | local_id | TPN_ID | closed_at             |
| -------- | -------- | -----: | --------------------- |
| Orbis    | ORB_001  | 123456 | `2026-09-24 10:30:00` |

---

# 21. Step 2 — Build the Baseline

Prioritisation is replayed without the previous Orbis matching contribution.

Refinitiv becomes the selected source.

| TPN_ID | VAT           | Selection            | closed_at             |
| -----: | ------------- | -------------------- | --------------------- |
| 123456 | VAT_ORBIS_OLD | `matching_orbis`     | `2026-09-24 10:31:00` |
| 123456 | VAT_REFINITIV | `matching_refinitiv` | `NULL`                |

The baseline is now established.

---

# 22. Step 3 — Replay the Updated Provider

The updated Orbis record is passed to the standard matching process.

Assume it matches again to:

```text
TPN_ID = 123456
```

The matching table now contains:

| Provider | local_id | TPN_ID | closed_at             |
| -------- | -------- | -----: | --------------------- |
| Orbis    | ORB_001  | 123456 | `2026-09-24 10:30:00` |
| Orbis    | ORB_001  | 123456 | `NULL`                |

The first row represents the previous matching.

The second row represents the matching obtained from the updated provider values.

---

# 23. Step 4 — Standard Cluster Processing

The standard cluster calculation is executed.

If the resulting cluster remains unchanged, the process continues.

If it changes, the impacted previous cluster rows are closed and the newly calculated cluster state is inserted.

Example:

### Previous

```text
C001
├── TPN 123456
└── TPN 456789
```

### After Replay

```text
C002
├── TPN 123456
└── TPN 987654
```

Result:

| Cluster | TPN_ID | closed_at             |
| ------- | -----: | --------------------- |
| C001    | 123456 | `2026-09-24 10:35:00` |
| C001    | 456789 | `2026-09-24 10:35:00` |
| C002    | 123456 | `NULL`                |
| C002    | 987654 | `NULL`                |

---

# 24. Step 5 — Replay Standard Prioritisation

The standard prioritisation logic is now executed.

The updated Orbis VAT:

```text
VAT_ORBIS_NEW
```

is once again available as a candidate.

If Orbis becomes the selected source again, the baseline Refinitiv value is historised.

### Baseline

| TPN_ID | VAT           | Selection            | closed_at |
| -----: | ------------- | -------------------- | --------- |
| 123456 | VAT_REFINITIV | `matching_refinitiv` | `NULL`    |

### Final Result

| TPN_ID | VAT           | Selection            | closed_at             |
| -----: | ------------- | -------------------- | --------------------- |
| 123456 | VAT_REFINITIV | `matching_refinitiv` | `2026-09-24 10:36:00` |
| 123456 | VAT_ORBIS_NEW | `matching_orbis`     | `NULL`                |

The Tax value has therefore changed twice during the processing of the same update.

---

# 25. Before / Baseline / Final View

| Stage             | Matching                               | Tax value     | Tax selection        | Cluster                                |
| ----------------- | -------------------------------------- | ------------- | -------------------- | -------------------------------------- |
| **Before update** | ORB_001 → 123456                       | VAT_ORBIS_OLD | `matching_orbis`     | C001                                   |
| **Baseline**      | Previous Orbis contribution removed    | VAT_REFINITIV | `matching_refinitiv` | Previous state retained for comparison |
| **Final state**   | ORB_001 → 123456 after standard replay | VAT_ORBIS_NEW | `matching_orbis`     | Result of standard cluster calculation |

The lifecycle is therefore:

```text
BEFORE

ORB_001
   ↓
TPN 123456
   ↓
VAT_ORBIS_OLD


BASELINE

Previous ORB_001 matching removed
   ↓
Prioritisation recalculated
   ↓
VAT_REFINITIV


STANDARD REPLAY

Updated ORB_001
   ↓
Matching
   ↓
Cluster Calculation
   ↓
Prioritisation
   ↓
VAT_ORBIS_NEW


FINAL

VAT_ORBIS_NEW
```

---

# 26. Alternative Outcome — No New Match

The updated provider record may no longer match any TPN.

In that case:

```text
Initial
VAT_ORBIS_OLD

      ↓

Baseline
VAT_REFINITIV

      ↓

Standard matching replay
No Orbis match

      ↓

Final
VAT_REFINITIV
```

The baseline therefore becomes the final state.

---

# 27. Alternative Outcome — Matching to Another TPN

Another possible outcome is:

```text
Before

ORB_001 → TPN 123456
```

After replay:

```text
ORB_001 → TPN 789012
```

In this scenario:

* `TPN_ID 123456` must retain the valid state obtained after removing the previous Orbis contribution;
* `TPN_ID 789012` receives the new Orbis matching contribution;
* impacted clusters must be recalculated;
* prioritisation must run normally for the resulting state.

---

# 28. Complete Sequence Diagram

```mermaid
sequenceDiagram

    participant P as External Provider
    participant D as Delta Processing
    participant MT as Matching Table
    participant PR as Prioritisation
    participant DS as Downstream Tables
    participant M as Standard Matching
    participant C as Cluster Processing
    participant CS as Cluster Stat

    P->>D: update_matching(local_id, updated values)

    D->>MT: Search current matching for local_id

    alt local_id is not currently matched

        MT-->>D: No matching found

        D->>M: Process updated provider normally
        M-->>D: Matching result

        D->>C: Standard cluster calculation
        C->>CS: Apply resulting cluster state

        D->>PR: Standard prioritisation
        PR->>DS: Apply Tax / Identification results

    else local_id is currently matched

        MT-->>D: Return previous TPN_ID

        D->>CS: Retrieve previous cluster state
        CS-->>D: Previous cluster information

        D->>DS: Retrieve current downstream values

        D->>MT: Close previous matching

        D->>PR: Recalculate without old provider contribution
        PR->>DS: Determine baseline values

        alt Baseline differs from current value
            PR->>DS: Close previous value
            PR->>DS: Insert baseline value
        else Baseline value unchanged
            PR-->>DS: Keep current value
        end

        Note over D,DS: Baseline reached

        D->>M: Submit updated provider to standard matching chain

        Note over M,DS: Standard processing starts here

        M->>M: Standard matching
        M-->>D: New matching result

        D->>C: Standard cluster calculation
        C->>CS: Compare previous and new cluster state

        alt Cluster changed
            CS->>CS: Close obsolete cluster rows
            CS->>CS: Insert new cluster state
        else Cluster unchanged
            CS-->>D: Keep current cluster state
        end

        D->>PR: Standard prioritisation
        PR->>DS: Re-evaluate downstream values

        alt Final value differs from baseline
            PR->>DS: Close baseline value
            PR->>DS: Insert final selected value
        else Baseline remains valid
            PR-->>DS: Keep baseline value
        end
    end
```

---

# 29. Comparison of Both Update Types

| Processing Step                                      | `update_identification`   | `update_matching`         |
| ---------------------------------------------------- | ------------------------- | ------------------------- |
| Check whether `local_id` is already matched          | Yes                       | Yes                       |
| Replay matching                                      | No                        | Yes                       |
| Close previous matching                              | No                        | Yes, when already matched |
| Recalculate cluster                                  | No                        | Yes                       |
| Reconcile impacted clusters                          | No                        | If required               |
| Replay prioritisation                                | Yes                       | Yes                       |
| Exclude previous provider contribution before replay | No                        | Yes                       |
| Build intermediate baseline                          | No                        | Yes                       |
| Replay complete standard chain afterwards            | No                        | Yes                       |
| Historise replaced downstream values                 | If prioritisation changes | If impacted               |

---

# 30. Core Processing Principle

For an `update_matching`, the process must clearly separate two responsibilities.

## Delta-specific processing

The delta logic must:

1. retrieve the existing matching and related state;
2. close the previous matching;
3. remove the downstream impact of the previous provider contribution;
4. replay the relevant prioritisation;
5. establish a consistent baseline.

## Standard processing

Once the baseline has been established, the updated provider record must go through the standard chain:

```text
Matching
   ↓
Cluster Calculation
   ↓
Prioritisation
   ↓
Tax / Identification
```

The delta process should prepare a clean baseline and then reuse the existing standard processing chain.

---

# 31. Summary

### `update_identification`

> Keep the existing matching and cluster, retrieve the associated `TPN_ID`, and replay only the impacted prioritisation logic.

### `update_matching`

> Remove the impact of the previous provider matching, establish a consistent baseline, then process the updated provider record through the complete standard matching, clustering and prioritisation chain. The baseline is only an intermediate state and may change again after the standard replay.

---

# 32. Simplified Final View

```mermaid
flowchart LR

    A[Provider Delta] --> B{Update Type}

    B -->|Identification| C{local_id already matched?}

    C -->|No| D[No action]
    C -->|Yes| E[Keep Matching]
    E --> F[Keep Cluster]
    F --> G[Replay impacted prioritisation]
    G --> H[Final State]

    B -->|Matching| I{local_id already matched?}

    I -->|No| J[Standard Matching Chain]

    I -->|Yes| K[Close Previous Matching]
    K --> L[Build Baseline]
    L --> M[Standard Matching]
    M --> N[Standard Cluster Calculation]
    N --> O[Standard Prioritisation]
    O --> P[Final State]
```
