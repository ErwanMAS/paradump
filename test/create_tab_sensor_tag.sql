CREATE TABLE `sensor_tag` (
  `id` bigint(20) unsigned NOT NULL,
  `lang_iso3` varchar(6) DEFAULT NULL,
  `label` varchar(128) DEFAULT NULL,
  `hex_label` varchar(64) DEFAULT NULL,
  KEY `id` (`id`,`lang_iso3`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ;

