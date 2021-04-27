
DROP TABLE IF EXISTS dataforce_sandbox.vb_cookie_test;
CREATE TABLE IF NOT EXISTS dataforce_sandbox.vb_cookie_test
(
    dt   varchar,
    visit_id int,
    placement varchar(2000),
    container varchar(2000),
    attribute varchar(2000),
    result varchar(2000)
) DISTSTYLE ALL;

INSERT INTO dataforce_sandbox.vb_cookie_test
SELECT dt, visit_id, placement, container, attribute, result
FROM s3_audience.publisher
WHERE destination = 'PS_IPLAYER' AND dt =20210410 LIMIT 1;