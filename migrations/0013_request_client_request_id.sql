-- Client correlation: the validated `X-Request-ID` a client sent with the
-- request (1-128 chars of [A-Za-z0-9._:-]). The gateway's own id remains
-- `requests.id` (the `api_call_id`, returned as `x-llmconduit-request-id`).
-- Nullable: legacy rows and requests without the header have none.

ALTER TABLE requests ADD COLUMN client_request_id TEXT;
CREATE INDEX IF NOT EXISTS idx_requests_client_request_id
    ON requests(client_request_id);
