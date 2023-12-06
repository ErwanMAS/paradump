CREATE TABLE `account_metadatas` (
  `clientid` bigint unsigned NOT NULL,
  `metatype` enum('password','ssh-key','gpg-key') NOT NULL,
  `metavalue` varbinary(512) DEFAULT NULL,
  `metasha256` varchar(66) DEFAULT NULL,
  PRIMARY KEY (`clientid`,`metatype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ;
