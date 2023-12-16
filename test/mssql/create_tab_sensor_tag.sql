CREATE TABLE $DB.sensor_tag (
  id bigint NOT NULL,
  lang_iso3 varchar(6) DEFAULT NULL,
  label varchar(128) DEFAULT NULL,
  hex_label varchar(64) DEFAULT NULL
)   ;

create index ix_sensor_tag_id_lang_iso3 on $DB.sensor_tag (id,lang_iso3) ;



