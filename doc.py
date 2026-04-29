stateDiagram-v2
    [*] --> UNDEFINED: tiers reçu

    UNDEFINED --> PROVIDED_BY_RPMP: Matching Component\nTVA RPMP présente
    UNDEFINED --> MATCHING_ORBIS: Matching Component\npas TVA RPMP + match Orbis
    UNDEFINED --> MATCHING_REFINITIV: Matching Component\npas TVA RPMP/Orbis + match Refinitiv
    UNDEFINED --> NOT_FOUND: Batch Collect\naucune TVA exploitable
    UNDEFINED --> NOT_ELIGIBLE: Batch Collect\ncalcul impossible

    PROVIDED_BY_RPMP --> CONFIRMED_BY_VIES: Batch Collect\nTVA RPMP validée par VIES
    PROVIDED_BY_RPMP --> MATCHING_ORBIS: MAJ tiers\nTVA RPMP supprimée + match Orbis
    PROVIDED_BY_RPMP --> MATCHING_REFINITIV: MAJ tiers\nTVA RPMP supprimée + match Refinitiv
    PROVIDED_BY_RPMP --> NOT_FOUND: MAJ tiers\nTVA RPMP supprimée + aucun match
    PROVIDED_BY_RPMP --> NOT_ELIGIBLE: MAJ tiers\nTVA RPMP supprimée + calcul impossible

    MATCHING_ORBIS --> CONFIRMED_BY_ORBIS: Batch Collect\nTVA Orbis validée
    MATCHING_ORBIS --> MATCHING_REFINITIV: MAJ matching\nmatch Orbis perdu + match Refinitiv présent
    MATCHING_ORBIS --> NOT_FOUND: MAJ matching\nmatch Orbis perdu + aucun match
    MATCHING_ORBIS --> PROVIDED_BY_RPMP: MAJ RPMP\nTVA RPMP ajoutée

    MATCHING_REFINITIV --> CONFIRMED_BY_REFINITIV: Batch Collect\nTVA Refinitiv validée
    MATCHING_REFINITIV --> MATCHING_ORBIS: MAJ matching\nmatch Orbis trouvé
    MATCHING_REFINITIV --> NOT_FOUND: MAJ matching\nmatch Refinitiv perdu
    MATCHING_REFINITIV --> PROVIDED_BY_RPMP: MAJ RPMP\nTVA RPMP ajoutée

    CONFIRMED_BY_VIES --> PROVIDED_BY_RPMP: MAJ RPMP\nnouvelle TVA RPMP
    CONFIRMED_BY_VIES --> MATCHING_ORBIS: MAJ données\ncalcul invalide + match Orbis
    CONFIRMED_BY_VIES --> MATCHING_REFINITIV: MAJ données\ncalcul invalide + match Refinitiv
    CONFIRMED_BY_VIES --> NOT_FOUND: MAJ données\nVIES ne trouve plus
    CONFIRMED_BY_VIES --> NOT_ELIGIBLE: MAJ données\ncalcul impossible

    CONFIRMED_BY_ORBIS --> PROVIDED_BY_RPMP: MAJ RPMP\nTVA RPMP ajoutée
    CONFIRMED_BY_ORBIS --> MATCHING_REFINITIV: MAJ matching\nmatch Orbis perdu + Refinitiv présent
    CONFIRMED_BY_ORBIS --> CONFIRMED_BY_REFINITIV: Batch Collect\nRefinitiv validé après perte Orbis
    CONFIRMED_BY_ORBIS --> NOT_FOUND: MAJ matching\nOrbis perdu + rien d'autre
    CONFIRMED_BY_ORBIS --> NOT_ELIGIBLE: MAJ données\ncalcul impossible + aucun match

    CONFIRMED_BY_REFINITIV --> PROVIDED_BY_RPMP: MAJ RPMP\nTVA RPMP ajoutée
    CONFIRMED_BY_REFINITIV --> MATCHING_ORBIS: MAJ matching\nmatch Orbis trouvé
    CONFIRMED_BY_REFINITIV --> CONFIRMED_BY_ORBIS: Batch Collect\nOrbis validé
    CONFIRMED_BY_REFINITIV --> NOT_FOUND: MAJ matching\nRefinitiv perdu + rien d'autre
    CONFIRMED_BY_REFINITIV --> NOT_ELIGIBLE: MAJ données\ncalcul impossible + aucun match

    NOT_FOUND --> PROVIDED_BY_RPMP: MAJ RPMP\nTVA RPMP ajoutée
    NOT_FOUND --> MATCHING_ORBIS: MAJ matching\nmatch Orbis trouvé
    NOT_FOUND --> MATCHING_REFINITIV: MAJ matching\nmatch Refinitiv trouvé
    NOT_FOUND --> CONFIRMED_BY_VIES: Batch Collect\ncalcul VIES valide
    NOT_FOUND --> REMEDIATION_ONGOING: remédiation démarrée

    NOT_ELIGIBLE --> PROVIDED_BY_RPMP: MAJ RPMP\nTVA RPMP ajoutée
    NOT_ELIGIBLE --> MATCHING_ORBIS: MAJ matching\nmatch Orbis trouvé
    NOT_ELIGIBLE --> MATCHING_REFINITIV: MAJ matching\nmatch Refinitiv trouvé
    NOT_ELIGIBLE --> NOT_FOUND: données redevenues calculables\nmais VIES not found

    REMEDIATION_ONGOING --> CONFIRMED_FOUND_MANUAL: utilisateur confirme TVA
    REMEDIATION_ONGOING --> CONFIRMED_BLANK_MANUAL: utilisateur confirme absence TVA
    REMEDIATION_ONGOING --> PROVIDED_BY_RPMP: MAJ RPMP prioritaire
    REMEDIATION_ONGOING --> MATCHING_ORBIS: match Orbis trouvé
    REMEDIATION_ONGOING --> MATCHING_REFINITIV: match Refinitiv trouvé

    MATCHING_ORBIS --> REF_ERROR: erreur appel référentiel
    MATCHING_REFINITIV --> REF_ERROR: erreur appel référentiel
    PROVIDED_BY_RPMP --> REF_ERROR: erreur technique
    CONFIRMED_BY_VIES --> REF_ERROR: erreur technique

    REF_ERROR --> UNDEFINED: reprise traitement
