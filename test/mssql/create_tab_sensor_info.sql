
create table $DB.sensor_info (
       id                decimal(20) NOT NULL ,
       model             int  NOT NULL ,
       hardware_id       varchar(24) ,
       mfg_date          datetime2 NOT NULL ,
       PRIMARY KEY ( id )
)    ;
GO

create index ix_sensor_info_hardware_id on $DB.sensor_info ( hardware_id ) ;
create index ix_sensor_info_mfg_date on $DB.sensor_info ( mfg_date ) ;


