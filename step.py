-- ============================================================
-- UM - TARGET (DDL simple)
-- PostgreSQL
-- Toutes les tables : business_valid_from/to + data_created/updated/closed
-- Tous les champs (hors ids/dates) : VARCHAR
-- ============================================================

-- -------------------------
-- Helpers: "dates standard"
-- (répétées ci-dessous volontairement pour rester simple)
-- business_valid_from DATE NOT NULL
-- business_valid_to   DATE
-- data_created_at     TIMESTAMP NOT NULL DEFAULT now()
-- data_updated_at     TIMESTAMP NOT NULL DEFAULT now()
-- data_closed_at      TIMESTAMP
-- -------------------------

-- =========================
-- 1) BNPP ENTITY
-- =========================
CREATE TABLE bnpp_entity (
  bnpp_entity_id      BIGSERIAL PRIMARY KEY,
  entity_id           VARCHAR(255),

  business_valid_from DATE NOT NULL,
  business_valid_to   DATE,
  data_created_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_updated_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_closed_at      TIMESTAMP
);

-- =========================
-- 2) BUSINESS GROUP
-- =========================
CREATE TABLE business_group (
  business_group_id        BIGSERIAL PRIMARY KEY,
  business_group_code      VARCHAR(255),
  business_group_type      VARCHAR(255),

  business_valid_from DATE NOT NULL,
  business_valid_to   DATE,
  data_created_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_updated_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_closed_at      TIMESTAMP
);

-- =========================
-- 3) LEGAL ENTITY STATIC DATA (CENTRAL)
-- =========================
CREATE TABLE legal_entity_static_data (
  legal_entity_id          BIGSERIAL PRIMARY KEY,
  tpn_id                   VARCHAR(255) UNIQUE,   -- clé métier utile

  -- tout le reste en varchar (comme demandé)
  company_name             VARCHAR(255),
  local_name               VARCHAR(255),
  long_name                VARCHAR(255),
  short_name               VARCHAR(255),
  aka                      VARCHAR(255),

  incorporation_legal_registration_country VARCHAR(255),
  incorporation_date       VARCHAR(255),
  number_of_employees      VARCHAR(255),
  legal_form               VARCHAR(255),

  lei_recertification_date VARCHAR(255),
  lei_registration_date    VARCHAR(255),
  lei_status               VARCHAR(255),

  direct_parent_lei        VARCHAR(255),
  ultimate_parent_lei      VARCHAR(255),

  local_activity_code_type VARCHAR(255),
  local_activity_code      VARCHAR(255),
  legal_nafc_code_2        VARCHAR(255),
  legal_nafc_code_2_1      VARCHAR(255),

  tp_relation_owner        VARCHAR(255),
  country_of_business      VARCHAR(255),
  typology_code            VARCHAR(255),
  industry_code            VARCHAR(255),
  pct_turnover             VARCHAR(255),
  type_of_shareholders     VARCHAR(255),
  type_of_clients          VARCHAR(255),
  client_coverage          VARCHAR(255),
  main_establishment       VARCHAR(255),
  legal_status             VARCHAR(255),
  tp_registration_status   VARCHAR(255),
  ihm_registration_flag    VARCHAR(255),
  rmpm_existence_flag      VARCHAR(255),
  data_certification_status VARCHAR(255),
  data_source              VARCHAR(255),
  listed_company_yn        VARCHAR(255),
  legal_document_link      VARCHAR(255),
  banking_secrecy_yn       VARCHAR(255),
  certification_data_owner VARCHAR(255),
  tp_registration_requester VARCHAR(255),

  -- lien optionnel vers business group (0,1)
  business_group_id        BIGINT REFERENCES business_group(business_group_id),

  business_valid_from DATE NOT NULL,
  business_valid_to   DATE,
  data_created_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_updated_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_closed_at      TIMESTAMP
);

CREATE INDEX idx_legal_entity_tpn_id ON legal_entity_static_data(tpn_id);

-- =========================
-- 4) COMPANY IDENTIFIER DATA (0,n)
-- =========================
CREATE TABLE company_identifier_data (
  company_identifier_data_id BIGSERIAL PRIMARY KEY,
  legal_entity_id            BIGINT NOT NULL REFERENCES legal_entity_static_data(legal_entity_id),

  company_identifier         VARCHAR(255),
  company_identifier_name    VARCHAR(255),
  company_identifier_nature  VARCHAR(255),
  company_identifier_source  VARCHAR(255),

  business_valid_from DATE NOT NULL,
  business_valid_to   DATE,
  data_created_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_updated_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_closed_at      TIMESTAMP
);

CREATE INDEX idx_company_identifier_le ON company_identifier_data(legal_entity_id);

-- =========================
-- 5) TP LIFECYCLE STEPS
-- =========================
CREATE TABLE tp_lifecycle_steps (
  tp_lifecycle_steps_id BIGSERIAL PRIMARY KEY,
  tp_lifecycle_steps    VARCHAR(255),

  business_valid_from DATE NOT NULL,
  business_valid_to   DATE,
  data_created_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_updated_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_closed_at      TIMESTAMP
);

-- =========================
-- 6) RELATIONSHIP ROLE WITH ENTITY (lien BNPP <-> LE) (0,n)
-- =========================
CREATE TABLE relationship_role_with_entity (
  relationship_role_id        BIGSERIAL PRIMARY KEY,
  legal_entity_id             BIGINT NOT NULL REFERENCES legal_entity_static_data(legal_entity_id),
  bnpp_entity_id              BIGINT NOT NULL REFERENCES bnpp_entity(bnpp_entity_id),

  relationship_role_with_entity VARCHAR(255),
  tp_status_with_bnpp_entity    VARCHAR(255),

  business_valid_from DATE NOT NULL,
  business_valid_to   DATE,
  data_created_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_updated_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_closed_at      TIMESTAMP
);

CREATE INDEX idx_rel_role_le ON relationship_role_with_entity(legal_entity_id);
CREATE INDEX idx_rel_role_bnpp ON relationship_role_with_entity(bnpp_entity_id);

-- =========================
-- 7) LINKED FAST-TRACK DATA (0,n)
-- =========================
CREATE TABLE linked_fast_track_data (
  linked_fast_track_id BIGSERIAL PRIMARY KEY,
  legal_entity_id      BIGINT NOT NULL REFERENCES legal_entity_static_data(legal_entity_id),

  tpn_id_fast_track    VARCHAR(255),

  business_valid_from DATE NOT NULL,
  business_valid_to   DATE,
  data_created_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_updated_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_closed_at      TIMESTAMP
);

CREATE INDEX idx_fast_track_le ON linked_fast_track_data(legal_entity_id);

-- =========================
-- 8) TP LIFECYCLE EVENT DATA (0,n)
-- =========================
CREATE TABLE tp_lifecycle_event_data (
  tp_lifecycle_event_id BIGSERIAL PRIMARY KEY,

  master_legal_entity_id      BIGINT NOT NULL REFERENCES legal_entity_static_data(legal_entity_id),
  subordinate_legal_entity_id BIGINT REFERENCES legal_entity_static_data(legal_entity_id),

  lifecycle_event_label VARCHAR(255),
  event_status          VARCHAR(255),

  business_valid_from DATE NOT NULL,
  business_valid_to   DATE,
  data_created_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_updated_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_closed_at      TIMESTAMP
);

CREATE INDEX idx_lifecycle_event_master ON tp_lifecycle_event_data(master_legal_entity_id);
CREATE INDEX idx_lifecycle_event_sub ON tp_lifecycle_event_data(subordinate_legal_entity_id);

-- =========================
-- 9) TAX DATA (0,n)
-- =========================
CREATE TABLE tax_data (
  tax_data_id       BIGSERIAL PRIMARY KEY,
  legal_entity_id   BIGINT NOT NULL REFERENCES legal_entity_static_data(legal_entity_id),

  tax_id            VARCHAR(255),
  tax_id_status     VARCHAR(255),
  tax_label         VARCHAR(255),

  business_valid_from DATE NOT NULL,
  business_valid_to   DATE,
  data_created_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_updated_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_closed_at      TIMESTAMP
);

CREATE INDEX idx_tax_le ON tax_data(legal_entity_id);

-- =========================
-- 10) TYPE OF ADDRESS DATA
-- =========================
CREATE TABLE type_of_address_data (
  type_of_address_id BIGSERIAL PRIMARY KEY,
  type_of_address    VARCHAR(255),

  business_valid_from DATE NOT NULL,
  business_valid_to   DATE,
  data_created_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_updated_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_closed_at      TIMESTAMP
);

-- =========================
-- 11) ADDRESS DATA (0,n)
-- =========================
CREATE TABLE address_data (
  address_id          BIGSERIAL PRIMARY KEY,
  legal_entity_id     BIGINT NOT NULL REFERENCES legal_entity_static_data(legal_entity_id),
  type_of_address_id  BIGINT NOT NULL REFERENCES type_of_address_data(type_of_address_id),

  address_distribution VARCHAR(255),
  address_way          VARCHAR(255),
  address_hamlet       VARCHAR(255),
  address_postcode_city VARCHAR(255),
  address_country      VARCHAR(255),

  business_valid_from DATE NOT NULL,
  business_valid_to   DATE,
  data_created_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_updated_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_closed_at      TIMESTAMP
);

CREATE INDEX idx_address_le ON address_data(legal_entity_id);
CREATE INDEX idx_address_type ON address_data(type_of_address_id);

-- =========================
-- 12) ISIN EQUITY DATA (0,n)
-- =========================
CREATE TABLE isin_equity_data (
  isin_equity_id   BIGSERIAL PRIMARY KEY,
  legal_entity_id  BIGINT NOT NULL REFERENCES legal_entity_static_data(legal_entity_id),

  isin_equity      VARCHAR(255),

  business_valid_from DATE NOT NULL,
  business_valid_to   DATE,
  data_created_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_updated_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_closed_at      TIMESTAMP
);

CREATE INDEX idx_isin_le ON isin_equity_data(legal_entity_id);

-- =========================
-- 13) INSTRUMENT DATA (0,n)
-- =========================
CREATE TABLE instrument_data (
  instrument_data_id   BIGSERIAL PRIMARY KEY,
  legal_entity_id      BIGINT NOT NULL REFERENCES legal_entity_static_data(legal_entity_id),

  instrument_id        VARCHAR(255),
  instrument_codification VARCHAR(255),
  instrument_nature    VARCHAR(255),

  business_valid_from DATE NOT NULL,
  business_valid_to   DATE,
  data_created_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_updated_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_closed_at      TIMESTAMP
);

CREATE INDEX idx_instrument_le ON instrument_data(legal_entity_id);

-- =========================
-- 14) CAPITALISTIC LINKS (0,n) : lien LE -> LE
-- =========================
CREATE TABLE capitalistic_links (
  capitalistic_link_id BIGSERIAL PRIMARY KEY,

  -- "nature of attachment" + "LE of superior attachment"
  owner_legal_entity_id       BIGINT NOT NULL REFERENCES legal_entity_static_data(legal_entity_id),
  superior_legal_entity_id    BIGINT REFERENCES legal_entity_static_data(legal_entity_id),

  nature_of_attachment        VARCHAR(255),

  business_valid_from DATE NOT NULL,
  business_valid_to   DATE,
  data_created_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_updated_at     TIMESTAMP NOT NULL DEFAULT now(),
  data_closed_at      TIMESTAMP
);

CREATE INDEX idx_cap_owner ON capitalistic_links(owner_legal_entity_id);
CREATE INDEX idx_cap_superior ON capitalistic_links(superior_legal_entity_id);
