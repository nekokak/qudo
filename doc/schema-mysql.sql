CREATE TABLE func (
    id         INT UNSIGNED PRIMARY KEY NOT NULL AUTO_INCREMENT,
    name       VARCHAR(255) NOT NULL,
    UNIQUE(name)
);
CREATE TABLE job (
    id              BIGINT UNSIGNED PRIMARY KEY NOT NULL AUTO_INCREMENT,
    func_id         INT UNSIGNED NOT NULL,
    arg             MEDIUMBLOB,
    uniqkey         VARCHAR(255) NULL,
    enqueue_time    INTEGER UNSIGNED,
    grabbed_until   INTEGER UNSIGNED NOT NULL,
    retry_cnt       INTEGER UNSIGNED NOT NULL DEFAULT 0,
    UNIQUE(func_id, uniqkey)
);
CREATE TABLE exception_log (
    id              BIGINT UNSIGNED PRIMARY KEY NOT NULL AUTO_INCREMENT,
    func_id         INT UNSIGNED NOT NULL DEFAULT 0,
    exception_time  INTEGER UNSIGNED NOT NULL,
    message         MEDIUMBLOB NOT NULL,
    uniqkey         VARCHAR(255) NULL,
    arg             MEDIUMBLOB,
    INDEX (func_id),
    INDEX (exception_time)
);

