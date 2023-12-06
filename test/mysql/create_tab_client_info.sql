CREATE TABLE `client_info` (
  `id` 	     		bigint(20) unsigned NOT NULL,
  `email`    		varchar(255) ,
  `status` 		tinyint(3) NOT NULL,
  `insert_ts`	timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP ,
  `update_ts`   timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP ,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
