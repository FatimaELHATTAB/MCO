sequenceDiagram
    autonumber

    participant BC as PROCESS<br/>Batch Collect
    participant UI as PROCESS<br/>update_identification
    participant TQ as TABLE<br/>task_queue
    participant LE as TABLE<br/>legal_entity
    participant MT as TABLE<br/>matching
    participant PR as PROCESS<br/>Prioritization

    BC->>TQ: Insert an update notification

    UI->>TQ: Read notifications to process
    TQ-->>UI: Return update_identification tasks

    Note over UI: Current process today<br/>No full matching replay<br/>No LEI / LUI / ISIN / fuzzy rules are re-executed

    UI->>LE: Identify impacted legal entities
    LE-->>UI: Return impacted rows

    loop For each impacted legal entity
        UI->>LE: Read the current active row
        LE-->>UI: Existing active row

        UI->>LE: Close the current active row
        Note over LE: Update legal_entity<br/>business_valid_to / data_closed_at

        UI->>MT: Retrieve already existing matches
        MT-->>UI: Return available values by source

        UI->>PR: Replay prioritization only
        PR-->>UI: Return selected priority value

        UI->>LE: Insert / update the new active row
        Note over LE: legal_entity is also the final updated table
    end
