# Matching and Cluster Lifecycle

## 1. Scope

| Item                     | Description                                                        |
| ------------------------ | ------------------------------------------------------------------ |
| Objective                | Build and maintain a shared identification repository              |
| Central identifier       | `TPNID`                                                            |
| Source systems           | RMPM, RCX, OSR                                                     |
| Source record identifier | `source + local_id`                                                |
| Matching trigger         | New record or identification data update                           |
| Manual review trigger    | Several records or several TPNIDs involved in an ambiguous result  |
| History principle        | Previous matching and cluster states are closed, never overwritten |

---

## 2. Main Objects

| Object             | Example    | Purpose                                                 |
| ------------------ | ---------- | ------------------------------------------------------- |
| Source record      | `RCX / B1` | Identification record received from a source            |
| TPNID              | `TP1`      | Central identifier of the real entity                   |
| Matching           | `B1 → TP1` | Association between a source record and a TPNID         |
| Cluster hash       | `H(B1,C1)` | Hash calculated from the cluster members                |
| Cluster version    | `CV4`      | Unique occurrence of a cluster during a validity period |
| Matching execution | `EXEC_005` | Execution that calculated or recalculated the matching  |

---

## 3. High-Level Architecture

```mermaid
flowchart LR
    subgraph Sources
        RMPM[RMPM]
        RCX[RCX]
        OSR[OSR]
    end

    RMPM --> ING[Data ingestion]
    RCX --> ING
    OSR --> ING

    ING --> DET{New record<br/>or update?}
    DET --> ME[Matching engine]

    ID[(Identification database<br/>TPNID)]
    ME <--> ID

    ME --> RES{Matching result}

    RES -->|No match| REG[Create new TPNID]
    RES -->|Unique match| CONF[Confirm association]
    RES -->|Ambiguous result| REM[Manual remediation]

    REM --> DEC[Business decision]

    REG --> MH[(Matching history)]
    CONF --> MH
    DEC --> MH

    MH --> CL[(Cluster versions)]
```

---

## 4. Logical Data Model

```mermaid
erDiagram
    SOURCE_RECORD {
        string source
        string local_id
        string identification_data
        string record_status
        datetime last_update_date
    }

    TPN_ENTITY {
        string tpn_id
        string entity_status
        datetime creation_date
    }

    MATCHING_HISTORY {
        string matching_id
        string source
        string local_id
        string tpn_id
        string cluster_version_id
        string matching_status
        string decision_type
        string execution_id
        datetime valid_from
        datetime valid_to
    }

    CLUSTER_VERSION {
        string cluster_version_id
        string cluster_hash
        string cluster_status
        string previous_cluster_version_id
        string change_type
        datetime valid_from
        datetime valid_to
    }

    CLUSTER_MEMBER {
        string cluster_version_id
        string source
        string local_id
    }

    SOURCE_RECORD ||--o{ MATCHING_HISTORY : generates
    TPN_ENTITY ||--o{ MATCHING_HISTORY : referenced_by
    CLUSTER_VERSION ||--o{ MATCHING_HISTORY : groups
    CLUSTER_VERSION ||--o{ CLUSTER_MEMBER : contains
    SOURCE_RECORD ||--o{ CLUSTER_MEMBER : belongs_to
```

---

## 5. Cluster Versioning Rule

```mermaid
flowchart LR
    C1["CV1<br/>H(B1,B2)<br/>TO_REVIEW"]
    C2["CV2<br/>H(B1)<br/>ACTIVE"]
    C3["CV3<br/>H(B2)<br/>ACTIVE"]
    C4["CV4<br/>H(B1,C1)<br/>ACTIVE"]

    C1 -->|Manual decision:<br/>B1 retained| C2
    C1 -->|Manual decision:<br/>B2 separated| C3
    C2 -->|C1 added| C4
```

| Event                        | Current cluster         | Action               | New cluster                         |
| ---------------------------- | ----------------------- | -------------------- | ----------------------------------- |
| Cluster creation             | None                    | Create version       | `CV1 / H(B1,B2)`                    |
| Member added                 | `CV2 / H(B1)`           | Close CV2            | `CV4 / H(B1,C1)`                    |
| Member removed               | `CV4 / H(B1,C1)`        | Close CV4            | `CV5 / H(C1)`                       |
| Cluster split                | `CV1 / H(B1,B2)`        | Close CV1            | `CV2 / H(B1)` and `CV3 / H(B2)`     |
| Cluster merge                | `H(B1,C1)` + `H(B2)`    | Close both versions  | `H(B1,B2,C1)`                       |
| Same composition reactivated | Previous version closed | Create a new version | Same hash, new `cluster_version_id` |

---

# 6. Reference Lifecycle

## 6.1 Timeline

```mermaid
timeline
    title Reference matching lifecycle

    Day 1 : A1 received from RMPM
          : No existing match
          : TP1 created

    Day 2 : B1 and B2 received from RCX
          : B1 matches TP1
          : B2 matches TP1
          : CV1 = H(B1,B2) created
          : Manual review required

    Day 3 : Business retains B1 for TP1
          : B2 is considered a different entity
          : CV1 closed
          : CV2 = H(B1) created
          : TP2 created for B2
          : CV3 = H(B2) created

    Day 4 : C1 received from OSR
          : C1 matches TP1
          : CV2 closed
          : CV4 = H(B1,C1) created
```

---

## 6.2 State by Step

| Step | Incoming data     | Matching result     | TPNID state          | Cluster action            |
| ---- | ----------------- | ------------------- | -------------------- | ------------------------- |
| D1   | `RMPM / A1`       | No match            | Create `TP1`         | No duplicate cluster      |
| D2   | `RCX / B1`        | Matches `TP1`       | TP1 unchanged        | Create pending cluster    |
| D2   | `RCX / B2`        | Matches `TP1`       | TP1 unchanged        | Add B2 to pending cluster |
| D3   | Business decision | B1 retained         | `TP1 = A1 + B1`      | Close `CV1`, create `CV2` |
| D3   | Business decision | B2 rejected for TP1 | Create `TP2`         | Create `CV3`              |
| D4   | `OSR / C1`        | Matches `TP1`       | `TP1 = A1 + B1 + C1` | Close `CV2`, create `CV4` |

---

## 6.3 Reference Sequence Diagram

```mermaid
sequenceDiagram
    autonumber

    participant SRC as Source systems
    participant ME as Matching engine
    participant ID as Identification DB
    participant MH as Matching history
    participant UI as Remediation UI

    rect rgb(245,245,245)
        Note over SRC,UI: Day 1 — Initial registration
        SRC->>ME: A1 / RMPM
        ME->>ID: Search existing entity
        ID-->>ME: No match
        ME->>ID: Create TP1
        ME->>ID: Associate A1 with TP1
    end

    rect rgb(245,245,245)
        Note over SRC,UI: Day 2 — Duplicate candidates
        SRC->>ME: B1 / RCX
        SRC->>ME: B2 / RCX

        ME->>ID: Match B1
        ID-->>ME: Candidate TP1

        ME->>ID: Match B2
        ID-->>ME: Candidate TP1

        ME->>MH: Create CV1 = H(B1,B2)
        ME->>MH: B1 → TP1 / TO_REVIEW
        ME->>MH: B2 → TP1 / TO_REVIEW

        MH->>UI: Submit duplicate cluster
    end

    rect rgb(245,245,245)
        Note over SRC,UI: Day 3 — Manual decision
        UI-->>MH: Confirm B1 → TP1
        UI-->>MH: Reject B2 → TP1

        MH->>MH: Close CV1
        ME->>ID: Create TP2 for B2

        ME->>MH: Create CV2 = H(B1)
        ME->>MH: Create CV3 = H(B2)
    end

    rect rgb(245,245,245)
        Note over SRC,UI: Day 4 — New OSR record
        SRC->>ME: C1 / OSR
        ME->>ID: Match C1
        ID-->>ME: TP1

        ME->>MH: Close CV2
        ME->>MH: Create CV4 = H(B1,C1)
        ME->>MH: C1 → TP1 / MATCHED
    end
```

---

# 7. Matching History — Reference Scenario

| Matching ID | Local ID | Source | TPNID | Cluster version | Cluster hash | Status     | Valid from | Valid to | Decision  |
| ----------- | -------- | ------ | ----- | --------------- | ------------ | ---------- | ---------- | -------- | --------- |
| M001        | B1       | RCX    | TP1   | CV1             | H(B1,B2)     | TO_REVIEW  | D2         | D3       | Automatic |
| M002        | B2       | RCX    | TP1   | CV1             | H(B1,B2)     | TO_REVIEW  | D2         | D3       | Automatic |
| M003        | B1       | RCX    | TP1   | CV2             | H(B1)        | MATCHED    | D3         | D4       | Manual    |
| M004        | B2       | RCX    | TP2   | CV3             | H(B2)        | REGISTERED | D3         | Current  | Manual    |
| M005        | B1       | RCX    | TP1   | CV4             | H(B1,C1)     | MATCHED    | D4         | Current  | System    |
| M006        | C1       | OSR    | TP1   | CV4             | H(B1,C1)     | MATCHED    | D4         | Current  | Automatic |

---

# 8. Reference State

## 8.1 Identification Database

| TPNID | Source | Local ID | Status |
| ----- | ------ | -------- | ------ |
| TP1   | RMPM   | A1       | ACTIVE |
| TP1   | RCX    | B1       | ACTIVE |
| TP1   | OSR    | C1       | ACTIVE |
| TP2   | RCX    | B2       | ACTIVE |

## 8.2 Active Matching State

| Local ID | Source | TPNID | Cluster version | Cluster hash | Status     |
| -------- | ------ | ----- | --------------- | ------------ | ---------- |
| B1       | RCX    | TP1   | CV4             | H(B1,C1)     | MATCHED    |
| C1       | OSR    | TP1   | CV4             | H(B1,C1)     | MATCHED    |
| B2       | RCX    | TP2   | CV3             | H(B2)        | REGISTERED |

---

# 9. Lifecycle Scenario Matrix

| ID  | Trigger                  | Previous state              | New matching result                  | TPNID action           | Cluster action                                | Review      |
| --- | ------------------------ | --------------------------- | ------------------------------------ | ---------------------- | --------------------------------------------- | ----------- |
| S01 | B1 updated               | B1 → TP1                    | B1 still matches TP1                 | No change              | No change                                     | No          |
| S02 | B1 updated               | B1 → TP1                    | No match                             | Create TP3             | Remove B1 from TP1 cluster; create B1 cluster | To validate |
| S03 | B1 updated               | B1 → TP1                    | B1 matches TP2                       | Move B1 to TP2         | Update both source and destination clusters   | Recommended |
| S04 | B1 updated               | B1 → TP1                    | Several candidate TPNIDs             | No immediate change    | Create review cluster                         | Yes         |
| S05 | B2 updated               | B2 → TP2                    | B2 now matches TP1                   | Possible TP1/TP2 merge | Merge clusters after decision                 | Yes         |
| S06 | B2 updated               | B2 → TP2                    | B2 still matches TP2                 | No change              | No change                                     | No          |
| S07 | C1 updated               | C1 → TP1                    | C1 still matches TP1                 | No change              | No change                                     | No          |
| S08 | C1 updated               | C1 → TP1                    | No match                             | Create new TPNID       | Remove C1 from TP1 cluster                    | To validate |
| S09 | C1 updated               | C1 → TP1                    | C1 matches another TPNID             | Move C1                | Update both clusters                          | Recommended |
| S10 | C1 becomes inactive      | C1 → TP1                    | Record inactive                      | TPNID unchanged        | Remove or mark inactive                       | Open        |
| S11 | C1 reactivated           | Previous matching closed    | C1 matches TP1 again                 | Restore association    | Create new cluster version                    | No          |
| S12 | D1 received              | New record                  | D1 matches TP1                       | Add D1 to TP1          | Close current cluster; add D1                 | No          |
| S13 | D1 and D2 received       | New records                 | Both match TP1                       | Pending decision       | Create duplicate review cluster               | Yes         |
| S14 | A1 updated               | A1 created TP1              | A1 still represents TP1              | No change              | Recalculate related matching                  | No          |
| S15 | A1 updated               | A1 created TP1              | A1 no longer represents TP1          | Potential reassignment | Possible split                                | Yes         |
| S16 | All members inactive     | TP1 has no active local ID  | No active supporting record          | Set TP1 status         | Close active cluster                          | Open        |
| S17 | Same composition returns | H(B1,C1) existed previously | B1 and C1 active again               | No TPNID change        | Same hash, new version ID                     | No          |
| S18 | Matching rule changes    | Existing active match       | Different result after recalculation | Depends on result      | Version affected clusters                     | Depends     |

---

# 10. Scenario Diagrams

## 10.1 B1 Still Matches TP1

```mermaid
flowchart LR
    A["B1 updated<br/>B1 → TP1<br/>CV4 = H(B1,C1)"]
    B[Matching recalculation]
    C["B1 still matches TP1"]
    D["TP1 unchanged<br/>CV4 unchanged"]

    A --> B --> C --> D
```

| Object      | Before               | After              |
| ----------- | -------------------- | ------------------ |
| B1 matching | TP1                  | TP1                |
| TPNID       | TP1                  | TP1                |
| Cluster     | CV4 / H(B1,C1)       | CV4 / H(B1,C1)     |
| History     | Existing association | New execution only |

---

## 10.2 B1 No Longer Matches Any TPNID

```mermaid
flowchart LR
    A["TP1<br/>A1 + B1 + C1"]
    B["CV4<br/>H(B1,C1)"]
    C[B1 updated]
    D[No match]
    E["TP1<br/>A1 + C1"]
    F["TP3<br/>B1"]
    G["CV5<br/>H(C1)"]
    H["CV6<br/>H(B1)"]

    A --> C
    B --> C
    C --> D
    D --> E
    D --> F
    E --> G
    F --> H
```

| Action          | Result           |
| --------------- | ---------------- |
| Close matching  | `B1 → TP1`       |
| Close cluster   | `CV4 / H(B1,C1)` |
| Create TPNID    | `TP3`            |
| Create matching | `B1 → TP3`       |
| Create cluster  | `CV5 / H(C1)`    |
| Create cluster  | `CV6 / H(B1)`    |

---

## 10.3 B1 Moves From TP1 to TP2

```mermaid
flowchart LR
    subgraph Before
        TP1A["TP1<br/>A1 + B1 + C1"]
        TP2A["TP2<br/>B2"]
        C4["CV4<br/>H(B1,C1)"]
        C3["CV3<br/>H(B2)"]
    end

    U[B1 identification update]

    subgraph After
        TP1B["TP1<br/>A1 + C1"]
        TP2B["TP2<br/>B1 + B2"]
        C5["CV5<br/>H(C1)"]
        C7["CV7<br/>H(B1,B2)"]
    end

    TP1A --> U
    TP2A --> U
    C4 --> U
    C3 --> U

    U --> TP1B
    U --> TP2B
    U --> C5
    U --> C7
```

| Closed           | Created          |
| ---------------- | ---------------- |
| `B1 → TP1`       | `B1 → TP2`       |
| `CV4 / H(B1,C1)` | `CV5 / H(C1)`    |
| `CV3 / H(B2)`    | `CV7 / H(B1,B2)` |

---

## 10.4 B2 Creates a Possible TP1/TP2 Merge

```mermaid
flowchart TD
    A["Current state<br/>B2 → TP2"]
    B[B2 updated]
    C["B2 now matches TP1"]
    D{Business decision}

    D -->|Different entities| E["Keep TP1 and TP2"]
    D -->|Same entity| F["Merge TP1 and TP2"]
    F --> G["Select surviving TPNID"]
    G --> H["Create merged cluster<br/>H(B1,B2,C1)"]

    A --> B --> C --> D
```

| Decision           | TPNID result              | Cluster result                      |
| ------------------ | ------------------------- | ----------------------------------- |
| Different entities | TP1 and TP2 remain active | No structural change                |
| Same entity        | One TPNID retained        | Existing clusters closed and merged |
| Uncertain          | No change                 | Review cluster remains pending      |

---

## 10.5 C1 Becomes Inactive and Reactivates

```mermaid
stateDiagram-v2
    [*] --> ActiveCV4

    ActiveCV4: CV4 = H(B1,C1)
    ActiveCV4: C1 ACTIVE

    ActiveCV4 --> ActiveCV8: C1 becomes inactive

    ActiveCV8: CV8 = H(B1)
    ActiveCV8: C1 matching closed

    ActiveCV8 --> ActiveCV12: C1 is received again and matches TP1

    ActiveCV12: CV12 = H(B1,C1)
    ActiveCV12: Same hash, new version
```

| Period      | Cluster version | Cluster hash | Members | Status |
| ----------- | --------------- | ------------ | ------- | ------ |
| D4–D10      | CV4             | H(B1,C1)     | B1, C1  | CLOSED |
| D10–D20     | CV8             | H(B1)        | B1      | CLOSED |
| D20–Current | CV12            | H(B1,C1)     | B1, C1  | ACTIVE |

---

# 11. Matching State Model

```mermaid
stateDiagram-v2
    [*] --> Candidate

    Candidate --> ToReview: Ambiguous result
    Candidate --> Matched: Unique valid match
    Candidate --> Registered: No existing match

    ToReview --> Matched: Match confirmed
    ToReview --> Registered: New TPNID created
    ToReview --> Rejected: Candidate rejected

    Matched --> Matched: Update, same TPNID
    Matched --> Closed: Matching becomes invalid
    Registered --> Closed: Association becomes invalid

    Closed --> Candidate: Record received again
```

| Status     | Meaning                                |
| ---------- | -------------------------------------- |
| CANDIDATE  | Result produced by the matching engine |
| TO_REVIEW  | Manual decision required               |
| MATCHED    | Record linked to an existing TPNID     |
| REGISTERED | New TPNID created for the record       |
| REJECTED   | Candidate association rejected         |
| CLOSED     | Association no longer valid            |

---

# 12. Cluster State Model

```mermaid
stateDiagram-v2
    [*] --> Active

    Active --> Closed: Member added or removed
    Active --> Closed: Split
    Active --> Closed: Merge
    Active --> Closed: No active member

    Closed --> [*]

    Active --> PendingReview: Ambiguous matching
    PendingReview --> Closed: Decision completed
    PendingReview --> Active: No structural change
```

| Change type | Previous cluster                  | New cluster                        |
| ----------- | --------------------------------- | ---------------------------------- |
| CREATE      | None                              | New active version                 |
| ADD         | Closed                            | New version with added member      |
| REMOVE      | Closed                            | New version without removed member |
| SPLIT       | Closed                            | Two or more new versions           |
| MERGE       | Multiple closed versions          | One new merged version             |
| REACTIVATE  | Historical version remains closed | New version with the same hash     |

---

# 13. Architecture Decisions and Open Points

| ID    | Decision / Question                                         | Proposed approach                                        | Status    |
| ----- | ----------------------------------------------------------- | -------------------------------------------------------- | --------- |
| AD-01 | Is a cluster immutable?                                     | Yes; close and create a new version                      | Confirmed |
| AD-02 | Can several Local IDs reference one TPNID?                  | Yes, including from the same source                      | Confirmed |
| AD-03 | What triggers matching recalculation?                       | Any identification data update                           | Confirmed |
| AD-04 | When is a new TPNID created?                                | No existing match found                                  | Confirmed |
| AD-05 | Is manual review required for ambiguous duplicates?         | Yes                                                      | Confirmed |
| AD-06 | Which TPNID survives a merge?                               | Business rule required                                   | Open      |
| AD-07 | Can a Local ID move automatically between TPNIDs?           | Manual review recommended                                | Open      |
| AD-08 | Are inactive Local IDs part of the active cluster hash?     | To be defined                                            | Open      |
| AD-09 | What happens when a TPNID has no active Local ID?           | Status rule required                                     | Open      |
| AD-10 | Is A1 included in cluster composition?                      | Must be clarified                                        | Open      |
| AD-11 | Is a cluster created for one Local ID?                      | Must be clarified                                        | Open      |
| AD-12 | Does an unchanged matching result create a new history row? | Store execution separately; do not duplicate association | Proposed  |
| AD-13 | Can the same cluster hash be reused?                        | Yes, but with a new cluster version ID                   | Proposed  |
