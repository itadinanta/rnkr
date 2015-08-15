CREATE TABLE wal (
    id text,
    watermark bigint,
    seq bigint,
    scoredata text,
    PRIMARY KEY ((id, watermark), seq)
) WITH COMPACT STORAGE;

CREATE TABLE watermarks (
    id text,
    watermark bigint,
    pages int,
    PRIMARY KEY (id, watermark)
) WITH COMPACT STORAGE;

CREATE TABLE pages (
    id text,
    watermark bigint,
    page int,
    scoredata text,
    PRIMARY KEY ((id, watermark, page))
);

CREATE TABLE metadata (
    id text,
    sorting text,
    encoding text,
    comment text,
    page_size int,
    wal_size_limit int,
    wal_time_limit bigint,
    PRIMARY KEY(id)
);