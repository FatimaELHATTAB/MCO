flowchart TD

    A[Legal Entity to process]

    A --> B{VAT provided by RMPM?}

    B -->|Yes| C[STATUS = PROVIDED_BY_RMPM]

    B -->|No| D{VAT eligible?}

    D -->|No| O[STATUS = NOT_ELIGIBLE]

    D -->|Yes| E{VAT computable?}

    E -->|Yes| F[Compute VAT]
    F --> G[Call VIES]

    G -->|OK| H[Update TAX_CODE<br/>STATUS = CONFIRMED_BY_VIES]

    G -->|NOT_FOUND| I

    G -->|VIES_ERROR| X[Stop processing<br/>Keep current TAX_CODE and STATUS<br/>Store VIES_ERROR<br/>Notify CRDS]

    X -->|Retry Batch TVA| G

    E -->|No| I{Orbis match available?}

    I -->|Yes| J[STATUS = CONFIRMED_BY_ORBIS]

    I -->|No| K{Refinitiv match available?}

    K -->|Yes| L[STATUS = CONFIRMED_BY_REFINITIV]

    K -->|No| M[STATUS = NOT_FOUND]
