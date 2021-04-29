
/*DROP TABLE IF EXISTS dataforce_sandbox.vb_cookie_test;
CREATE TABLE IF NOT EXISTS dataforce_sandbox.vb_cookie_test
(
    dt   varchar,
    visit_id int,
    placement varchar(2000),
    container varchar(2000),
    attribute varchar(2000),
    result varchar(2000)
) DISTSTYLE ALL;*/

INSERT INTO dataforce_sandbox.vb_cookie_test
SELECT dt, visit_id  
FROM s3_audience.publisher
WHERE destination = 'PS_IPLAYER' AND dt = run_date LIMIT 1;

INSERT INTO dataforce_sandbox.vb_cookie_test
SELECT dt,visit_id
FROM s3_audience.publisher
WHERE destination = 'PS_IPLAYER' AND dt = previous_run_date LIMIT 1;

GRANT ALL ON dataforce_sandbox.vb_cookie_test TO vicky_banks ;