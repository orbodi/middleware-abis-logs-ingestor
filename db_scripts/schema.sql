-- Schéma de persistance pour abis_logs_ingestor
-- Cible : PostgreSQL 13+
-- Placeholder __DB_SCHEMA__ remplacé au runtime (voir db_config.py)
-- Aligné sur la prod ABIS (events + vues analytiques, sans biometric_scores)

CREATE SCHEMA IF NOT EXISTS __DB_SCHEMA__;

-- ---------------------------------------------------------------------------
-- Table principale
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS __DB_SCHEMA__.events (
    id              BIGSERIAL PRIMARY KEY,
    source_file     TEXT NOT NULL,
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
    request_message                   JSONB,
    brs_request                       JSONB,
    brs_response                      JSONB,
    response_published_to_mosip_queue JSONB,
    payload         JSONB NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Vues — insert / identify (déduplication)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW __DB_SCHEMA__.v_insert_requests AS
SELECT
    id,
    source_file,
    business_id,
    service,
    operation,
    event_timestamp,
    request_id,
    request_message
FROM __DB_SCHEMA__.events
WHERE operation = 'REQUEST_VALIDATION_FOR_MISSING_NODES_INPROCESS'
  AND request_message->>'id' = 'mosip.abis.insert';

CREATE OR REPLACE VIEW __DB_SCHEMA__.v_deduplication_requests AS
SELECT
    id,
    source_file,
    business_id,
    service,
    operation,
    event_timestamp,
    request_id,
    request_message
FROM __DB_SCHEMA__.events
WHERE operation = 'REQUEST_VALIDATION_FOR_MISSING_NODES_INPROCESS'
  AND request_message->>'id' = 'mosip.abis.identify';

-- Alias rétrocompatible (même définition que v_deduplication_requests)
CREATE OR REPLACE VIEW __DB_SCHEMA__.v_identify_requests AS
SELECT
    id,
    source_file,
    business_id,
    service,
    operation,
    event_timestamp,
    request_id,
    request_message
FROM __DB_SCHEMA__.events
WHERE operation = 'REQUEST_VALIDATION_FOR_MISSING_NODES_INPROCESS'
  AND request_message->>'id' = 'mosip.abis.identify';

CREATE OR REPLACE VIEW __DB_SCHEMA__.v_insert_error_cases AS
SELECT
    id,
    source_file,
    business_id,
    service,
    operation,
    event_timestamp,
    request_id,
    request_message,
    response_published_to_mosip_queue
FROM __DB_SCHEMA__.events
WHERE service = 'INSERT'
  AND response_published_to_mosip_queue->>'id' = 'mosip.abis.insert'
  AND response_published_to_mosip_queue->>'returnValue' = '2';

CREATE OR REPLACE VIEW __DB_SCHEMA__.v_identify_error_cases AS
SELECT
    id,
    source_file,
    business_id,
    service,
    operation,
    event_timestamp,
    request_id,
    request_message,
    response_published_to_mosip_queue
FROM __DB_SCHEMA__.events
WHERE request_message->>'id' = 'mosip.abis.identify'
  AND response_published_to_mosip_queue->>'id' = 'mosip.abis.identify'
  AND response_published_to_mosip_queue->>'returnValue' = '2';

-- ---------------------------------------------------------------------------
-- Vues — adjudication manuelle
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW __DB_SCHEMA__.v_adjudication_requests AS
SELECT
    id,
    source_file,
    business_id,
    service,
    operation,
    event_timestamp,
    request_id,
    request_message
FROM __DB_SCHEMA__.events
WHERE operation = 'REQUEST_VALIDATION_FOR_MISSING_NODES_INPROCESS'
  AND request_message->>'id' = 'mosip.manual.adjudication.adjudicate';

CREATE OR REPLACE VIEW __DB_SCHEMA__.v_adjudication_cases AS
SELECT
    id,
    source_file,
    business_id,
    service,
    operation,
    event_timestamp,
    request_id,
    response_published_to_mosip_queue
FROM __DB_SCHEMA__.events
WHERE response_published_to_mosip_queue->>'id' = 'mosip.manual.adjudication.adjudicate'
  AND response_published_to_mosip_queue->>'returnValue' = '1';

CREATE OR REPLACE VIEW __DB_SCHEMA__.v_adjudication_error_cases AS
SELECT
    id,
    source_file,
    business_id,
    service,
    operation,
    event_timestamp,
    request_id,
    response_published_to_mosip_queue
FROM __DB_SCHEMA__.events
WHERE response_published_to_mosip_queue->>'id' = 'mosip.manual.adjudication.adjudicate'
  AND response_published_to_mosip_queue->>'returnValue' = '2';

CREATE OR REPLACE VIEW __DB_SCHEMA__.v_adjudication_hit_cases AS
SELECT
    id,
    source_file,
    business_id,
    service,
    operation,
    event_timestamp,
    request_id,
    response_published_to_mosip_queue
FROM __DB_SCHEMA__.events
WHERE response_published_to_mosip_queue->>'id' = 'mosip.manual.adjudication.adjudicate'
  AND response_published_to_mosip_queue->>'returnValue' = '1'
  AND (response_published_to_mosip_queue->'candidateList'->>'count')::integer > 0;

CREATE OR REPLACE VIEW __DB_SCHEMA__.v_adjudication_nohit_cases AS
SELECT
    id,
    source_file,
    business_id,
    service,
    operation,
    event_timestamp,
    request_id,
    response_published_to_mosip_queue
FROM __DB_SCHEMA__.events
WHERE response_published_to_mosip_queue->>'id' = 'mosip.manual.adjudication.adjudicate'
  AND response_published_to_mosip_queue->>'returnValue' = '1'
  AND (response_published_to_mosip_queue->'candidateList'->>'count')::integer = 0;

CREATE OR REPLACE VIEW __DB_SCHEMA__.v_adjudication_pending_cases AS
SELECT
    r.request_id,
    r.event_timestamp
FROM __DB_SCHEMA__.v_adjudication_requests r
LEFT JOIN __DB_SCHEMA__.v_adjudication_cases t ON r.request_id = t.request_id
LEFT JOIN __DB_SCHEMA__.v_adjudication_error_cases e ON r.request_id = e.request_id
WHERE t.request_id IS NULL
  AND e.request_id IS NULL;

-- ---------------------------------------------------------------------------
-- Vue matérialisée — cas adjudication en attente
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS __DB_SCHEMA__.mv_adjudication_pending_cases AS
SELECT
    e.id,
    e.source_file,
    e.business_id,
    e.origin,
    e.origin_id,
    e.log_category,
    e.service,
    e.activity,
    e.activity_result,
    e.owner,
    e.host,
    e.event_timestamp,
    e.duration,
    e.operation,
    e.reference_id,
    e.request_id,
    e.request_time,
    e.response_time,
    e.brs_url,
    e.request_message,
    e.brs_request,
    e.brs_response,
    e.response_published_to_mosip_queue,
    e.payload,
    e.created_at
FROM __DB_SCHEMA__.events e
WHERE e.operation = 'REQUEST_VALIDATION_FOR_MISSING_NODES_INPROCESS'
  AND e.request_message->>'id' = 'mosip.manual.adjudication.adjudicate'
  AND NOT EXISTS (
      SELECT 1
      FROM __DB_SCHEMA__.events e1
      WHERE e1.request_id = e.request_id
        AND e1.response_published_to_mosip_queue->>'id' = 'mosip.manual.adjudication.adjudicate'
        AND e1.response_published_to_mosip_queue->>'returnValue' = '1'
  )
  AND NOT EXISTS (
      SELECT 1
      FROM __DB_SCHEMA__.events e2
      WHERE e2.request_id = e.request_id
        AND e2.response_published_to_mosip_queue->>'id' = 'mosip.manual.adjudication.adjudicate'
        AND e2.response_published_to_mosip_queue->>'returnValue' = '2'
  )
WITH NO DATA;

-- ---------------------------------------------------------------------------
-- Index — généraux
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_events_business_id
    ON __DB_SCHEMA__.events (business_id);

CREATE INDEX IF NOT EXISTS idx_events_service
    ON __DB_SCHEMA__.events (service);

CREATE INDEX IF NOT EXISTS idx_events_operation
    ON __DB_SCHEMA__.events (operation);

CREATE INDEX IF NOT EXISTS idx_events_event_timestamp
    ON __DB_SCHEMA__.events (event_timestamp);

CREATE INDEX IF NOT EXISTS idx_events_timestamp
    ON __DB_SCHEMA__.events (event_timestamp);

CREATE INDEX IF NOT EXISTS brin_events_event_timestamp
    ON __DB_SCHEMA__.events USING brin (event_timestamp);

CREATE INDEX IF NOT EXISTS idx_events_request_id
    ON __DB_SCHEMA__.events (request_id);

CREATE INDEX IF NOT EXISTS idx_events_request_message_id
    ON __DB_SCHEMA__.events ((request_message->>'id'));

CREATE INDEX IF NOT EXISTS idx_events_response_message_id
    ON __DB_SCHEMA__.events ((response_published_to_mosip_queue->>'id'));

CREATE INDEX IF NOT EXISTS idx_events_response_return_value
    ON __DB_SCHEMA__.events ((response_published_to_mosip_queue->>'returnValue'));

CREATE INDEX IF NOT EXISTS idx_events_time_request_id
    ON __DB_SCHEMA__.events (event_timestamp, (request_message->>'id'));

-- ---------------------------------------------------------------------------
-- Index — insert (mosip.abis.insert)
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_events_insert_requests
    ON __DB_SCHEMA__.events (operation, (request_message->>'id'));

CREATE INDEX IF NOT EXISTS idx_insert_requests_optimized
    ON __DB_SCHEMA__.events (event_timestamp, request_id)
    INCLUDE (operation, request_message)
    WHERE operation = 'REQUEST_VALIDATION_FOR_MISSING_NODES_INPROCESS'
      AND request_message->>'id' = 'mosip.abis.insert';

CREATE INDEX IF NOT EXISTS idx_events_insert_errors
    ON __DB_SCHEMA__.events (service, (response_published_to_mosip_queue->>'id'), (response_published_to_mosip_queue->>'returnValue'))
    WHERE service = 'INSERT';

CREATE INDEX IF NOT EXISTS idx_insert_error_date
    ON __DB_SCHEMA__.events (event_timestamp, request_id)
    INCLUDE (service, response_published_to_mosip_queue)
    WHERE service = 'INSERT'
      AND response_published_to_mosip_queue->>'id' = 'mosip.abis.insert'
      AND response_published_to_mosip_queue->>'returnValue' = '2';

-- ---------------------------------------------------------------------------
-- Index — identify / déduplication (mosip.abis.identify)
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_dedup_requests_optimized
    ON __DB_SCHEMA__.events (event_timestamp, request_id)
    INCLUDE (operation, request_message)
    WHERE operation = 'REQUEST_VALIDATION_FOR_MISSING_NODES_INPROCESS'
      AND request_message->>'id' = 'mosip.abis.identify';

CREATE INDEX IF NOT EXISTS idx_events_identify_errors
    ON __DB_SCHEMA__.events (
        (request_message->>'id'),
        (response_published_to_mosip_queue->>'id'),
        (response_published_to_mosip_queue->>'returnValue')
    );

CREATE INDEX IF NOT EXISTS idx_identify_error_cases_optimized
    ON __DB_SCHEMA__.events (event_timestamp, request_id)
    INCLUDE (id, source_file, business_id, service, operation, request_message, response_published_to_mosip_queue)
    WHERE request_message->>'id' = 'mosip.abis.identify'
      AND response_published_to_mosip_queue->>'id' = 'mosip.abis.identify'
      AND response_published_to_mosip_queue->>'returnValue' = '2';

-- ---------------------------------------------------------------------------
-- Index — adjudication (mosip.manual.adjudication.adjudicate)
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_events_adjudication_requests
    ON __DB_SCHEMA__.events (operation, (request_message->>'id'))
    WHERE request_message->>'id' = 'mosip.manual.adjudication.adjudicate';

CREATE INDEX IF NOT EXISTS idx_adjud_requests_optimized
    ON __DB_SCHEMA__.events (event_timestamp, request_id)
    INCLUDE (operation, request_message)
    WHERE operation = 'REQUEST_VALIDATION_FOR_MISSING_NODES_INPROCESS'
      AND request_message->>'id' = 'mosip.manual.adjudication.adjudicate';

CREATE INDEX IF NOT EXISTS idx_adjud_requests_covering
    ON __DB_SCHEMA__.events (event_timestamp)
    INCLUDE (id, request_id, operation, request_message)
    WHERE operation = 'REQUEST_VALIDATION_FOR_MISSING_NODES_INPROCESS'
      AND request_message->>'id' = 'mosip.manual.adjudication.adjudicate';

CREATE INDEX IF NOT EXISTS idx_events_adj_req_covering
    ON __DB_SCHEMA__.events (request_id)
    INCLUDE (event_timestamp)
    WHERE operation = 'REQUEST_VALIDATION_FOR_MISSING_NODES_INPROCESS'
      AND request_message->>'id' = 'mosip.manual.adjudication.adjudicate';

CREATE INDEX IF NOT EXISTS idx_events_adjudication_cases
    ON __DB_SCHEMA__.events (
        (response_published_to_mosip_queue->>'id'),
        (response_published_to_mosip_queue->>'returnValue')
    );

CREATE INDEX IF NOT EXISTS idx_events_adjudication_success
    ON __DB_SCHEMA__.events (
        (response_published_to_mosip_queue->>'id'),
        (response_published_to_mosip_queue->>'returnValue')
    )
    WHERE response_published_to_mosip_queue->>'id' = 'mosip.manual.adjudication.adjudicate'
      AND response_published_to_mosip_queue->>'returnValue' = '1';

CREATE INDEX IF NOT EXISTS idx_events_adj_success_reqid
    ON __DB_SCHEMA__.events (request_id)
    WHERE response_published_to_mosip_queue->>'id' = 'mosip.manual.adjudication.adjudicate'
      AND response_published_to_mosip_queue->>'returnValue' = '1';

CREATE INDEX IF NOT EXISTS idx_adjud_cases_optimized
    ON __DB_SCHEMA__.events (event_timestamp, request_id)
    INCLUDE (response_published_to_mosip_queue)
    WHERE response_published_to_mosip_queue->>'id' = 'mosip.manual.adjudication.adjudicate'
      AND response_published_to_mosip_queue->>'returnValue' = '1';

CREATE INDEX IF NOT EXISTS idx_events_adjudication_errors
    ON __DB_SCHEMA__.events (
        (response_published_to_mosip_queue->>'id'),
        (response_published_to_mosip_queue->>'returnValue')
    )
    WHERE response_published_to_mosip_queue->>'id' = 'mosip.manual.adjudication.adjudicate'
      AND response_published_to_mosip_queue->>'returnValue' = '2';

CREATE INDEX IF NOT EXISTS idx_events_adj_error_reqid
    ON __DB_SCHEMA__.events (request_id)
    WHERE response_published_to_mosip_queue->>'id' = 'mosip.manual.adjudication.adjudicate'
      AND response_published_to_mosip_queue->>'returnValue' = '2';

CREATE INDEX IF NOT EXISTS idx_adjud_error_cases_optimized
    ON __DB_SCHEMA__.events (event_timestamp, request_id)
    INCLUDE (id, source_file, business_id, service, operation, response_published_to_mosip_queue)
    WHERE response_published_to_mosip_queue->>'id' = 'mosip.manual.adjudication.adjudicate'
      AND response_published_to_mosip_queue->>'returnValue' = '2';

CREATE INDEX IF NOT EXISTS idx_adjud_hit_cases_optimized
    ON __DB_SCHEMA__.events (event_timestamp, request_id)
    INCLUDE (response_published_to_mosip_queue)
    WHERE response_published_to_mosip_queue->>'id' = 'mosip.manual.adjudication.adjudicate'
      AND response_published_to_mosip_queue->>'returnValue' = '1'
      AND (response_published_to_mosip_queue->'candidateList'->>'count')::integer > 0;

CREATE INDEX IF NOT EXISTS idx_events_adjudication_candidate_count
    ON __DB_SCHEMA__.events (((response_published_to_mosip_queue->'candidateList'->>'count')::integer))
    WHERE response_published_to_mosip_queue->>'id' = 'mosip.manual.adjudication.adjudicate'
      AND response_published_to_mosip_queue->>'returnValue' = '1';

CREATE INDEX IF NOT EXISTS idx_mv_adjud_pending_event_timestamp
    ON __DB_SCHEMA__.mv_adjudication_pending_cases (event_timestamp, request_id);
