

create table sensors_pairing (
       clientid           bigint(20)  unsigned NOT NULL ,
       sensorid           bigint(20)  unsigned NOT NULL ,
       pairing_code       bigint(20)  unsigned NULL ,
       activated_features bigint(20)  unsigned NOT NULL ,
       pairing_time datetime NOT NULL ,
       PRIMARY KEY (sensorid,clientid),
       KEY ix_clientid ( clientid )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT ;


