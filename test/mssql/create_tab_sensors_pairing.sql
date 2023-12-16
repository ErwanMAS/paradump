

create table $DB.sensors_pairing (
       clientid           bigint NOT NULL ,
       sensorid           decimal(20) NOT NULL ,
       pairing_code       decimal(20) NULL ,
       activated_features decimal(20) NOT NULL ,
       pairing_time 	  datetime2 NOT NULL ,
       PRIMARY KEY (sensorid,clientid)
)    ;

create index ix_sensors_pairing_clientid on $DB.sensors_pairing ( clientid ) ;




