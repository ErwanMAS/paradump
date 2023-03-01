
create table sensor_info (
       id                bigint(20)  unsigned NOT NULL ,
       model             int unsigned NOT NULL ,
       hardware_id       varchar(24) ,
       mfg_date          datetime NOT NULL ,
       PRIMARY KEY ( id ) ,
       KEY ix_hardware_id ( hardware_id ) ,
       KEY ix_mfg_date ( mfg_date )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT ;

