CREATE TABLE client_activity (
  clientid bigint NOT NULL,
  ticketid decimal(20) NOT NULL,
  state int NOT NULL,
  modified timestamptz NULL DEFAULT NULL,
  border smallint   DEFAULT '0',
  topic varchar(32) DEFAULT NULL,
  PRIMARY KEY (clientid,ticketid)
)  ;


create index ix_client_activity_ticketid on client_activity (ticketid ) ;
