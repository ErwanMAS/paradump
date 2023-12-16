
CREATE TABLE $DB.account_metadatas (
  clientid bigint NOT NULL,
  metatype varchar(12) NOT NULL CHECK (metatype IN('password', 'ssh-key', 'gpg-key')) ,
  metavalue varbinary(256) DEFAULT NULL,
  metasha256 varchar(66) DEFAULT NULL,
  PRIMARY KEY (clientid,metatype)
)   ;
