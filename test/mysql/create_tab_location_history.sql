CREATE TABLE `location_history` (
  `clientid` bigint(20) unsigned NOT NULL,
  `location_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `location_gps_x` float DEFAULT NULL,
  `location_gps_y` float DEFAULT NULL,
  `location_ip` varchar(64) DEFAULT NULL,
  KEY `ix_clientid_location_time` (`clientid`,`location_time`),
  KEY `ix_location_gps_x_location_gps_y` (`location_gps_x`,`location_gps_y`),
  KEY `ix_location_ip` (`location_ip`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ;
