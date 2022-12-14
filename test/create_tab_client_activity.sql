CREATE TABLE `client_activity` (
  `clientid` bigint(20) unsigned NOT NULL,
  `ticketid` bigint(20) unsigned NOT NULL,
  `state` int(11) NOT NULL,
  `modified` timestamp NULL DEFAULT NULL,
  `border` smallint(6) DEFAULT '0',
  `topic` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`clientid`,`ticketid`),
  KEY `ix_ticketid` (`ticketid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ;
