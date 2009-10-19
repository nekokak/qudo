CREATE TABLE func (
    id         SERIAL,
    name       VARCHAR(255) NOT NULL,
    UNIQUE(name)
);

CREATE TABLE job (
    id              SERIAL,
    func_id         INT NOT NULL,
    arg             BYTEA,
    uniqkey         VARCHAR(255) NULL,
    enqueue_time    INTEGER,
    grabbed_until   INTEGER  NOT NULL,
    run_after       INTEGER  NOT NULL DEFAULT 0,
    retry_cnt       INTEGER  NOT NULL DEFAULT 0,
    priority        INTEGER  NOT NULL DEFAULT 0,
    UNIQUE(func_id, uniqkey)
);

CREATE TABLE exception_log (
    id              SERIAL,
    func_id         INTEGER NOT NULL DEFAULT 0,
    exception_time  INTEGER NOT NULL,
    message         BYTEA,
    uniqkey         VARCHAR(255) NULL,
    arg             BYTEA,
    retried         SMALLINT,
);
CREATE INDEX exception_log_func_id ON exception_log (func_id);
CREATE INDEX exception_log_exception_time ON exception_log (exception_time);

CREATE TABLE job_status (
    id              SERIAL,
    func_id         INTEGER NOT NULL DEFAULT 0,
    arg             BYTEA,
    uniqkey         VARCHAR(255) NULL,
    status          VARCHAR(10),
    job_start_time  INTEGER NOT NULL,
    job_end_time    INTEGER NOT NULL
);

