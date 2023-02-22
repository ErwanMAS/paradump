CREATE TABLE `ticket_tag` (
  `id` 	       bigint(20) unsigned NOT NULL,
  `lang_iso3`     varchar(6) ,
  `label`         varchar(128) ,
  `label_hex_u8`  varchar(256) ,
  `label_hex_l1`  varchar(256) ,  
  PRIMARY KEY (`id`,lang_iso3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ;
