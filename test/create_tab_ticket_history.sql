CREATE TABLE `ticket_history` (
  `ticketid` bigint(20) unsigned NOT NULL,
  `state` int(11) NOT NULL,
  `modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `topic` varchar(32) DEFAULT NULL,
  `human_factor` double DEFAULT NULL,
  `category` int(10) DEFAULT NULL,
  `data` text NOT NULL,
  KEY `ix_ticketid` (`ticketid`),
  KEY `ix_modified` (`modified`),
  KEY `ix_state` (`state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ;
