CREATE TYPE account_metadatas_metatype AS ENUM ('password','ssh-key','gpg-key');

CREATE TABLE account_metadatas (
  clientid bigint NOT NULL,
  metatype account_metadatas_metatype NOT NULL,
  metavalue bytea DEFAULT NULL,
  metasha256 varchar(66) DEFAULT NULL,
  PRIMARY KEY (clientid,metatype)
)   ;
