# Duplicate Cluster Processing – Matching Arbitration

## 1. Purpose

The Duplicate Cluster Processing component is responsible for processing
manual decisions produced by the Matching Arbitration UI.

When duplicate matching candidates are identified, the cluster is submitted
for manual arbitration.

The arbitration decision is stored in the `matching` table through the
`dedup_decision` field.

Possible values are:

| Value | Meaning |
|------|---------|
| TRUE | The matching relationship has been validated |
| FALSE | The matching relationship has been rejected |
| NULL | No arbitration decision has been provided yet |

Once a decision is submitted, the UI creates a `MATCHING_ARBITRATION`
task.

The task does not directly contain the business decision to execute.

Instead, the Duplicate Cluster Processing component reloads the current
matching state from the database and determines the appropriate action.


## 2. Architecture

The arbitration logic is isolated in a dedicated component:

    DuplicateClusterProcessing

Its responsibility is to:

1. Retrieve the matching rows concerned by the arbitration.
2. Identify whether the source is internal or external.
3. Evaluate the current arbitration state.
4. Trigger the appropriate downstream process.


The Task Processor remains responsible only for dispatching the task:

    MATCHING_ARBITRATION
            |
            v
    DuplicateClusterProcessing
            |
            +---------------------+
            |                     |
            v                     v
     External Source        Internal Source


## 3. Configuration

Source classification, SQL queries and reusable filters are externalized
from the Python implementation.

They are stored in:

    config/duplicate_cluster_processing.json

This configuration contains:

- Internal source list
- External source list
- SQL queries
- Arbitration filters
- Target table configuration

Example:

    External sources:
        ORBIS
        REFINITIV

    Internal sources:
        RMPM
        RTX


This approach avoids hardcoding source-specific configuration throughout
the Python processing logic.


## 4. Arbitration Notification

A notification is generated with:

    task_type = MATCHING_ARBITRATION

Example payload:

    {
        "source": "REFINITIV",
        "tpn_ids": [
            "4AAV423119",
            "4AAG386002",
            "2433907334",
            "2378797468"
        ],
        "cluster_id":
        "640fdc58a9bce0a22d6647434d141bdb418c69eced30d8ea157da2bb4070f488"
    }

The notification identifies the arbitration scope.

However, downstream processing is always based on the current state stored
in the `matching` table.

The notification therefore acts as a trigger rather than as the source of
truth for the arbitration decision.


## 5. External Sources

External sources currently include:

    ORBIS
    REFINITIV

Two cases are handled.


### 5.1 FALSE Decision

When an external matching candidate has been rejected:

    dedup_decision = FALSE

no additional processing is required.

The candidate is simply considered rejected.


### 5.2 TRUE Decision

When an external matching candidate has been validated:

    dedup_decision = TRUE

the validated matching result continues through the standard downstream
matching mechanisms.

The objective is to reuse the existing processes rather than implementing
a separate arbitration-specific workflow.

The validated matching rows are forwarded to:

    VAT processing

and:

    Matching prioritization


Conceptually:

    External source
           |
           v
    Arbitration decision
           |
       +---+---+
       |       |
      TRUE    FALSE
       |       |
       v       v
    VAT +    No action
    Prioritization


## 6. Internal Sources

Internal sources require additional processing because a Local ID can have
multiple TPN matching candidates.

Example:

| Source | Local ID | TPN ID | Decision |
|--------|----------|--------|----------|
| RMPM | L001 | TPN001 | FALSE |
| RMPM | L001 | TPN002 | NULL |

A FALSE decision for TPN001 does not mean that Local ID L001 has been fully
rejected.

TPN002 is still waiting for an arbitration decision.

For this reason, the processing must evaluate all matching candidates
associated with the Local ID.


## 7. Internal Source – TRUE Decision

When at least one candidate is validated:

    dedup_decision = TRUE

the relationship between:

    TPN ID
    Local ID
    Source

is considered validated.

The corresponding association can then be propagated to the Campaign
Identifier table.

Example:

    TPN ID   = TPN001
    Local ID = 5005798076
    Source   = RMPM


The arbitration result becomes:

    TPN001
       |
       +--- RMPM / 5005798076


## 8. Internal Source – FALSE Decision With Pending Candidates

Consider:

| Local ID | TPN ID | Decision |
|----------|--------|----------|
| L001 | TPN001 | FALSE |
| L001 | TPN002 | NULL |

The first candidate has been rejected.

However, another candidate is still awaiting arbitration.

Therefore:

    No registration is triggered.

The process waits for the remaining arbitration decision.


## 9. Internal Source – All Candidates Rejected

Consider:

| Local ID | TPN ID | Decision |
|----------|--------|----------|
| L001 | TPN001 | FALSE |
| L001 | TPN002 | FALSE |

For this Local ID:

    TRUE count = 0
    NULL count = 0

All matching candidates have therefore been reviewed and rejected.

The Local ID cannot be associated with an existing TPN through the matching
process.

The registration / immatriculation process can then be triggered.


## 10. Important Processing Rule

The arbitration decision must not be evaluated only at cluster level.

For internal sources, all matching rows associated with the Local ID must
be retrieved before deciding whether registration can be started.

Incorrect approach:

    Current cluster = FALSE
            |
            v
       Registration


Correct approach:

    Current decision = FALSE
            |
            v
    Retrieve all candidates
    for the same Local ID
            |
            v
      +-----+------+
      |            |
    NULL exists   No NULL
      |            |
      v            v
     WAIT      Check TRUE
                    |
             +------+------+
             |             |
           TRUE        No TRUE
             |             |
             v             v
        Existing       Registration
         match


This prevents registration from being triggered while another matching
candidate is still waiting for arbitration.


## 11. Decision Matrix

| Source Type | Arbitration State | Action |
|-------------|------------------|--------|
| External | TRUE | VAT + Matching Prioritization |
| External | FALSE | No action |
| Internal | TRUE exists | Update Campaign Identifier |
| Internal | No TRUE + NULL exists | Wait |
| Internal | No TRUE + No NULL | Trigger Registration |


## 12. Processing Flow

    MATCHING_ARBITRATION
             |
             v
    DuplicateClusterProcessing
             |
       Identify source
             |
       +-----+------+
       |            |
       v            v
    External      Internal
       |            |
       |            +----------------------+
       |                                   |
       v                                   v
    TRUE/FALSE                       Load all rows
       |                             for Local ID
    +--+---+                                |
    |      |                         +------+------+
   TRUE   FALSE                      |             |
    |      |                       TRUE        No TRUE
    |      |                         |             |
    v      v                         v        +----+----+
  VAT +   STOP                  Campaign      |         |
  PRIO                         Identifier    NULL     No NULL
                                            exists       |
                                              |          |
                                              v          v
                                             WAIT    Registration


## 13. Design Principles

The implementation follows the following principles:

- The Task Processor only dispatches the arbitration task.
- Arbitration business rules are isolated in
  `DuplicateClusterProcessing`.
- SQL queries are externalized in configuration.
- Source classification is externalized in configuration.
- Reusable decision filters are externalized in configuration.
- Database state remains the source of truth.
- External validated matches reuse the existing matching pipeline.
- Internal decisions are evaluated at Local ID level.
- Registration is triggered only when all candidates have been rejected.


## 14. Summary

Matching Arbitration introduces a manual decision step within the existing
matching workflow.

The resulting behaviour is:

    External + TRUE
        -> VAT
        -> Matching Prioritization

    External + FALSE
        -> No action

    Internal + TRUE
        -> Campaign Identifier

    Internal + FALSE + pending NULL
        -> Wait

    Internal + all candidates FALSE
        -> Registration / Immatriculation

The main objective is to reuse the existing matching mechanisms while
keeping arbitration-specific rules isolated, configurable and easy to
maintain.
