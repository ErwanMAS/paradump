CREATE TABLE $DB.client_activity (
  clientid bigint NOT NULL,
  ticketid decimal(20) NOT NULL,
  state int NOT NULL,
  modified datetimeoffset  NULL DEFAULT NULL ,
  border smallint   DEFAULT '0',
  topic varchar(32) DEFAULT NULL,
  CONSTRAINT pk PRIMARY KEY (clientid,ticketid)
) ;
create index ix_client_activity_ticketid on $DB.client_activity (ticketid ) ;



