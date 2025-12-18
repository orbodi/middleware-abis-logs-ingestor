-- Schéma de persistance pour abis_logs_ingestor
-- Cible : PostgreSQL

-- Créer le schéma logique
CREATE SCHEMA IF NOT EXISTS abis;

-- Table principale des événements de log
CREATE TABLE IF NOT EXISTS abis.events (
    id              BIGSERIAL PRIMARY KEY,

    -- Métadonnées techniques
    source_file     TEXT NOT NULL,

    -- Champs principaux des logs ABIS (noms alignés sur le JSON généré)
    business_id     UUID,
    origin          TEXT,
    origin_id       TEXT,
    log_category    TEXT,
    service         TEXT,
    activity        TEXT,
    activity_result TEXT,
    owner           TEXT,
    host            TEXT,

    event_timestamp TIMESTAMPTZ,
    duration        INTEGER,
    operation       TEXT,
    reference_id    TEXT,
    request_id      TEXT,
    request_time    TIMESTAMPTZ,
    response_time   TIMESTAMPTZ,

    brs_url         TEXT,

    -- Sous-objets complexes conservés en JSONB
    request_message                 JSONB, -- champ REQUEST_MESSAGE du log
    brs_request                     JSONB, -- champ BRS_REQUEST
    brs_response                    JSONB, -- champ BRS_RESPONSE (s'il existe)
    response_published_to_mosip_queue JSONB, -- champ RESPONSE_PUBLISHED_TO_MOSIP_QUEUE (adjudication)

    -- Payload complet
    payload         JSONB NOT NULL,       -- objet JSON complet tel qu'ingéré/corrigé

    created_at      TIMESTAMPTZ DEFAULT now()
);

-- Index utiles pour les analyses
CREATE INDEX IF NOT EXISTS idx_events_business_id ON abis.events (business_id);
CREATE INDEX IF NOT EXISTS idx_events_service     ON abis.events (service);
CREATE INDEX IF NOT EXISTS idx_events_operation   ON abis.events (operation);
CREATE INDEX IF NOT EXISTS idx_events_timestamp   ON abis.events (event_timestamp);

-- Index fonctionnel pour les requêtes d'insertion MOSIP -> ABIS
-- (OPERATION = REQUEST_VALIDATION_FOR_MISSING_NODES_INPROCESS
--  et REQUEST_MESSAGE.id = 'mosip.abis.insert')
CREATE INDEX IF NOT EXISTS idx_events_insert_requests
    ON abis.events (operation, ((request_message->>'id')));


-- Vue pratique : événements correspondant aux requêtes d'insertion MOSIP -> ABIS
CREATE OR REPLACE VIEW abis.v_insert_requests AS
SELECT
    id,
    source_file,
    business_id,
    service,
    operation,
    event_timestamp,
    request_id,
    request_message
FROM abis.events
WHERE operation = 'REQUEST_VALIDATION_FOR_MISSING_NODES_INPROCESS'
  AND request_message->>'id' = 'mosip.abis.insert';


-- Vue pratique : événements correspondant aux requêtes d'identification MOSIP -> ABIS
CREATE OR REPLACE VIEW abis.v_identify_requests AS
SELECT
    id,
    source_file,
    business_id,
    service,
    operation,
    event_timestamp,
    request_id,
    request_message
FROM abis.events
WHERE operation = 'REQUEST_VALIDATION_FOR_MISSING_NODES_INPROCESS'
  AND request_message->>'id' = 'mosip.abis.identify';


