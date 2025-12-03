CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO users (email, status, created_at)
VALUES ('alice@example.com', 'ACTIVE', NOW() - INTERVAL '2 days'),
       ('bob@example.com', 'SUSPENDED', NOW() - INTERVAL '1 day'),
       ('carol@example.com', 'ACTIVE', NOW());
