
create table sensor_info (
       id                bigint NOT NULL ,
       model             int  NOT NULL ,
       hardware_id       varchar(24) ,
       mfg_date          timestamp NOT NULL ,
       PRIMARY KEY ( id )
)    ;

create index ix_sensor_info_hardware_id on sensor_info ( hardware_id ) ;
create index ix_sensor_info_mfg_date on sensor_info ( mfg_date ) ;

