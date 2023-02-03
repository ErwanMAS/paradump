CREATE TABLE `location_history` (
  `clientid` bigint(20) unsigned NOT NULL,
  `location_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `location_gps_x` float DEFAULT NULL,
  `location_gps_y` float DEFAULT NULL,
  `location_ip` varchar(64) DEFAULT NULL,
  KEY `ix_clientid_location_time` (`clientid`,`location_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ;
