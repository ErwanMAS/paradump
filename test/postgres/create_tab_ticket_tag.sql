CREATE TABLE ticket_tag (
  lang_iso3     varchar(6) ,
  id 	       bigint NOT NULL,
  label         varchar(128) ,
  label_hex_u8  varchar(256) ,
  label_hex_l1  varchar(256) ,
  label_postgres_hex_u8 varchar(256) ,
  label_hex_u16le varchar(256) ,
  PRIMARY KEY (id,lang_iso3)
)   ;
