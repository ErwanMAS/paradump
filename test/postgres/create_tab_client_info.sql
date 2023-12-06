SELECT
EXISTS ( SELECT setting
         FROM   pg_settings
         WHERE  name = 'server_version_num'
         AND    setting::int >= 120000
         AND    setting::int  < 130000
       )
       AS pg_version_12
, EXISTS ( SELECT setting
         FROM   pg_settings
         WHERE  name = 'server_version_num'
         AND    setting::int >= 110000
         AND    setting::int  < 120000
         )
         AS pg_version_11
, EXISTS ( SELECT setting
         FROM   pg_settings
         WHERE  name = 'server_version_num'
         AND    setting::int >= 100000
         AND    setting::int <  110000
         )
         AS pg_version_10
, EXISTS ( SELECT setting
         FROM   pg_settings
         WHERE  name = 'server_version_num'
         AND    setting::int < 100000
         )
         AS pg_version_less_than_10
, EXISTS ( SELECT setting
         FROM   pg_settings
         WHERE  name = 'server_version_num'
         AND    setting::int < 110000
         )
         AS pg_version_less_than_11
\gset

CREATE TABLE client_info (
  id 	     		bigint NOT NULL,
  email    		varchar(255) ,
  status 		smallint NOT NULL,
  insert_ts   timestamptz  NOT NULL DEFAULT CURRENT_TIMESTAMP ,
  update_ts   timestamptz  NOT NULL DEFAULT CURRENT_TIMESTAMP ,
  PRIMARY KEY (id)
)  ;

CREATE OR REPLACE FUNCTION tg_return_new_update_ts() RETURNS TRIGGER
LANGUAGE plpgsql
AS
$$
BEGIN
    NEW.update_ts = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;

\if :pg_version_less_than_11

CREATE TRIGGER tg_client_info_update_ts
    BEFORE UPDATE ON client_info
    FOR EACH ROW
    EXECUTE PROCEDURE tg_return_new_update_ts();

\else

CREATE TRIGGER tg_client_info_update_ts
    BEFORE UPDATE ON client_info
    FOR EACH ROW
    EXECUTE FUNCTION tg_return_new_update_ts();

\endif





