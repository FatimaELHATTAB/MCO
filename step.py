sequenceDiagram
    autonumber

    participant BC as Batch Collect
    participant TIR as Table données immatriculées<br/>(RPMP)
    participant EXT as Table sources externes<br/>(Orbis / Refinitiv)
    participant MATCH as UM-Matching
    participant LE as LEGAL_ENTITY
    participant BTVA as Batch TVA
    participant VIES as API VIES

    BC->>TIR: Insère / met à jour les tiers RPMP
    BC->>EXT: Insère / met à jour les données Orbis / Refinitiv

    MATCH->>TIR: Lit les données RPMP
    MATCH->>EXT: Lit les sources externes

    alt TVA présente dans RPMP
        MATCH->>LE: status = PROVIDED_BY_RPMP
    else Match Orbis trouvé
        MATCH->>LE: status = MATCHING_ORBIS
    else Match Refinitiv trouvé
        MATCH->>LE: status = MATCHING_REFINITIV
    else Aucune donnée exploitable
        MATCH->>LE: status = UNDEFINED / NOT_FOUND
    end

    BTVA->>LE: Lit LEGAL_ENTITY avec le statut courant

    alt status = PROVIDED_BY_RPMP
        BTVA->>VIES: Valide TVA RPMP
        VIES-->>BTVA: Valid / Not found / Error
        BTVA->>LE: status = CONFIRMED_BY_VIES / NOT_FOUND / VIES_ERROR

    else status = MATCHING_ORBIS
        BTVA->>VIES: Valide TVA Orbis
        VIES-->>BTVA: Valid / Not found / Error
        BTVA->>LE: status = CONFIRMED_BY_ORBIS / NOT_FOUND / VIES_ERROR

    else status = MATCHING_REFINITIV
        BTVA->>VIES: Valide TVA Refinitiv
        VIES-->>BTVA: Valid / Not found / Error
        BTVA->>LE: status = CONFIRMED_BY_REFINITIV / NOT_FOUND / VIES_ERROR

    else Aucun statut candidat exploitable
        BTVA->>BTVA: Calcule TVA depuis SIREN + country
        alt Calcul possible
            BTVA->>VIES: Valide TVA calculée
            VIES-->>BTVA: Valid / Not found / Error
            BTVA->>LE: status = CONFIRMED_BY_VIES / NOT_FOUND / VIES_ERROR
        else Calcul impossible
            BTVA->>LE: status = NOT_ELIGIBLE
        end
    end

    opt Mise à jour ultérieure
        BC->>TIR: Met à jour données RPMP
        BC->>EXT: Met à jour sources externes
        MATCH->>LE: Recalcule et écrase le statut candidat
        BTVA->>LE: Relit le statut courant
        BTVA->>VIES: Revalide si nécessaire
        BTVA->>LE: Met à jour le statut final
    end
