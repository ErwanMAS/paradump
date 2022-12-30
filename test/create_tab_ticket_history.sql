CREATE TABLE `ticket_history` (
  `ticketid` bigint(20) unsigned NOT NULL,
  `state` int(11) NOT NULL,
  `modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ,
  `topic` varchar(32) DEFAULT NULL,
  `human_factor` double DEFAULT NULL,
  PRIMARY KEY (`ticketid`,`modified`,`state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ;
