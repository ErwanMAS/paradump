

create table sensors_pairing (
       clientid           bigint NOT NULL ,
       sensorid           bigint NOT NULL ,
       pairing_code       bigint NULL ,
       activated_features bigint NOT NULL ,
       pairing_time timestamp NOT NULL ,
       PRIMARY KEY (sensorid,clientid)
)    ;

create index ix_sensors_pairing_clientid on sensors_pairing ( clientid ) ;




