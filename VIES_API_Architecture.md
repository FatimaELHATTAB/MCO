# Flexible Change-Based Action Triggering

## 1. Context

Today, some actions are already triggered through `task_queue`.

Example: `update_identification` does not replay the full matching.
It uses existing results from `matching` and replays only prioritization to update `legal_entity`.

New need: trigger actions when specific data changes are detected around `identity`.

Main related tables:

* `lifecycle`
* `address`
* `tax`
* `company_identifier`

Common key: `tpn_id`.

---

## 2. Need

Some data changes can have a business impact and must trigger a specific action.

First use case:

```text
lifecycle.bnpp_entity changes
old value = outside CRDS scope
new value = inside CRDS scope
→ trigger CRDS action
```

Expected action for this case:

```text
Insert CRDS_NOTIFICATION into task_queue with the impacted tpn_id.
```

But the need must stay generic.

```mermaid
flowchart TD

    ID[(TABLE<br/>identity)]
    LC[(TABLE<br/>lifecycle)]
    AD[(TABLE<br/>address)]
    TX[(TABLE<br/>tax)]
    CI[(TABLE<br/>company_identifier)]

    LC -->|bnpp_entity changed| CRDS[CRDS action needed]
    AD -->|address changed| ADDR[Address action needed]
    TX -->|tax data changed| TAX[Tax action needed]
    CI -->|identifier changed| IDENT[Identifier action needed]

    CRDS --> NEED[Need:<br/>detect the change<br/>and trigger the right action]
    ADDR --> NEED
    TAX --> NEED
    IDENT --> NEED
```

---

## 3. Proposed Solution

Introduce a generic mechanism:

```text
Data change detected
→ rule evaluation
→ configured action triggered
```

The action can be:

* create a task in `task_queue`
* launch a process
* update a table
* replay prioritization
* write an audit/log event

```mermaid
flowchart TD

    BC[PROCESS<br/>Batch Collect]

    ID[(TABLE<br/>identity)]
    LC[(TABLE<br/>lifecycle)]
    AD[(TABLE<br/>address)]
    TX[(TABLE<br/>tax)]
    CI[(TABLE<br/>company_identifier)]

    CD[PROCESS<br/>Change Detection]
    CFG[(TABLE<br/>change_action_rule)]
    RE[PROCESS<br/>Rule Evaluation]
    AE[PROCESS<br/>Action Execution]

    TQ[(TABLE<br/>task_queue)]
    DP[PROCESS<br/>Dedicated Process]
    TU[(TABLE<br/>Target Table)]
    LOG[(TABLE<br/>Audit / Log)]

    BC --> ID
    BC --> LC
    BC --> AD
    BC --> TX
    BC --> CI

    ID --> CD
    LC --> CD
    AD --> CD
    TX --> CD
    CI --> CD

    CD -->|tpn_id, table, column,<br/>old value, new value| RE
    CFG -->|configured rules| RE

    RE --> AE

    AE -->|TASK_QUEUE_NOTIFICATION| TQ
    AE -->|PROCESS_EXECUTION| DP
    AE -->|DATA_UPDATE| TU
    AE -->|AUDIT_ONLY| LOG
```

---

## 4. Configuration Table

Proposed table:

```text
change_action_rule
```

| Column                | Description                     |
| --------------------- | ------------------------------- |
| `rule_id`             | Technical rule ID               |
| `rule_name`           | Functional rule name            |
| `source_table`        | Table to monitor                |
| `source_column`       | Column to monitor               |
| `old_value_condition` | Condition on old value          |
| `new_value_condition` | Condition on new value          |
| `action_type`         | Type of action                  |
| `action_name`         | Action name                     |
| `target_process`      | Process to launch, if needed    |
| `target_table`        | Table to update, if needed      |
| `is_enabled`          | Rule activation flag            |
| `priority`            | Priority if several rules match |
| `created_at`          | Creation timestamp              |
| `updated_at`          | Last update timestamp           |

---

## 5. Example Rules

| rule_name                 | source_table         | source_column      | condition                                     | action_type               | action_name                 |
| ------------------------- | -------------------- | ------------------ | --------------------------------------------- | ------------------------- | --------------------------- |
| Entity enters CRDS scope  | `lifecycle`          | `bnpp_entity`      | Old outside CRDS scope, new inside CRDS scope | `TASK_QUEUE_NOTIFICATION` | `CRDS_NOTIFICATION`         |
| Address changed           | `address`            | `address_value`    | Any change                                    | `PROCESS_EXECUTION`       | `ADDRESS_UPDATE_PROCESS`    |
| Tax changed               | `tax`                | `tax_value`        | Any change                                    | `PROCESS_EXECUTION`       | `TAX_PROCESSING`            |
| Identifier changed        | `company_identifier` | `identifier_value` | Any change                                    | `PROCESS_EXECUTION`       | `IDENTIFIER_RECONCILIATION` |
| Legal entity data changed | `legal_entity`       | selected fields    | Any change                                    | `PRIORITIZATION_REPLAY`   | `UPDATE_IDENTIFICATION`     |

---

## 6. How It Works

When data is updated:

```text
1. Compare old value and new value
2. Detect relevant changes
3. Read enabled rules from change_action_rule
4. Check rule conditions
5. Trigger configured action
```

For CRDS:

```text
source_table = lifecycle
source_column = bnpp_entity
old_value_condition = NOT_IN_CRDS_SCOPE
new_value_condition = IN_CRDS_SCOPE
action_type = TASK_QUEUE_NOTIFICATION
action_name = CRDS_NOTIFICATION
```

Task created in `task_queue`:

```text
task_name      = CRDS_NOTIFICATION
status         = PENDING
tpn_id         = impacted tpn_id
source_table   = lifecycle
source_column  = bnpp_entity
old_value      = previous bnpp_entity value
new_value      = new bnpp_entity value
event_type     = BNPP_ENTITY_ENTERED_CRDS_SCOPE
created_at     = current timestamp
```

---

## 7. Action Types

| action_type               | Meaning                         |
| ------------------------- | ------------------------------- |
| `TASK_QUEUE_NOTIFICATION` | Insert a task into `task_queue` |
| `PROCESS_EXECUTION`       | Launch a dedicated process      |
| `DATA_UPDATE`             | Update a target table           |
| `PRIORITIZATION_REPLAY`   | Replay prioritization logic     |
| `AUDIT_ONLY`              | Write audit/log only            |

---

## 8. Generic Sequence Diagram

```mermaid
sequenceDiagram
    autonumber

    participant BC as PROCESS<br/>Batch Collect
    participant DT as TABLE<br/>Data tables
    participant CD as PROCESS<br/>Change Detection
    participant CFG as TABLE<br/>change_action_rule
    participant RE as PROCESS<br/>Rule Evaluation
    participant AE as PROCESS<br/>Action Execution
    participant TQ as TABLE<br/>task_queue
    participant DP as PROCESS<br/>Downstream Process

    BC->>DT: Load / update data
    BC->>CD: Send old and new values

    CD->>CD: Detect changed fields

    alt Relevant change detected
        CD->>RE: Send tpn_id, table, column, old value, new value
        RE->>CFG: Read matching rule
        CFG-->>RE: Return configured action

        alt Rule matched
            RE->>AE: Trigger action

            alt TASK_QUEUE_NOTIFICATION
                AE->>TQ: Insert task
            else PROCESS_EXECUTION
                AE->>DP: Launch process
            else DATA_UPDATE
                AE->>DT: Update target data
            end
        else No rule matched
            RE-->>CD: No action
        end

    else No relevant change
        CD-->>BC: No action
    end
```

---

## 9. CRDS Sequence Diagram

```mermaid
sequenceDiagram
    autonumber

    participant BC as PROCESS<br/>Batch Collect
    participant LC as TABLE<br/>lifecycle
    participant CD as PROCESS<br/>Change Detection
    participant CFG as TABLE<br/>change_action_rule
    participant RE as PROCESS<br/>Rule Evaluation
    participant TQ as TABLE<br/>task_queue
    participant CRDS as PROCESS<br/>CRDS Processing

    BC->>LC: Update lifecycle data
    BC->>CD: Send old and new values

    CD->>CD: Detect change on bnpp_entity

    alt bnpp_entity changed
        CD->>RE: Send tpn_id, old value, new value
        RE->>CFG: Read CRDS rule
        CFG-->>RE: Return rule configuration

        alt Entity entered CRDS scope
            RE->>TQ: Insert CRDS_NOTIFICATION with tpn_id
        else No CRDS scope entry
            RE-->>CD: No action
        end
    else No change
        CD-->>BC: No action
    end

    CRDS->>TQ: Read pending CRDS_NOTIFICATION
    TQ-->>CRDS: Return impacted tpn_id
```

---

## 10. Key Points

* CRDS is the first use case.
* The mechanism is generic.
* New rules can be added by configuration.
* The action is not always a notification.
* `tpn_id` is the key used to link the event to the impacted entity.
* The same logic can be reused for `address`, `tax`, `company_identifier`, or other future changes.
