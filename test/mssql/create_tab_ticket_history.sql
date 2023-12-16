CREATE TABLE $DB.ticket_history (
  ticketid decimal(20) NOT NULL,
  state int NOT NULL,
  modified datetimeoffset  NOT NULL DEFAULT SYSDATETIMEOFFSET(),
  topic varchar(32) DEFAULT NULL,
  human_factor double precision DEFAULT NULL,
  category int DEFAULT NULL,
  data text NOT NULL
)   ;
GO



create index ix_ticket_history_ticketid on $DB.ticket_history (ticketid) ;
create index ix_ticket_history_modified on $DB.ticket_history (modified) ;
create index ix_ticket_history_state on $DB.ticket_history (state) ;

