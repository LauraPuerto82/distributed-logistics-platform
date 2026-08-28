-- migrate:up

CREATE TABLE shipments (
    id VARCHAR(50) PRIMARY KEY,
    origin VARCHAR(100) NOT NULL,
    destination VARCHAR(100) NOT NULL,
    weight NUMERIC(10,2) NOT NULL CHECK (weight > 0),
    priority VARCHAR(10) NOT NULL
        CHECK (priority IN ('LOW', 'MEDIUM', 'HIGH')),
    status VARCHAR(30) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (origin <> destination)
);

-- migrate:down

DROP TABLE shipments;