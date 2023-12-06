CREATE TABLE ticket_history (
  ticketid bigint NOT NULL,
  state int NOT NULL,
  modified timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  topic varchar(32) DEFAULT NULL,
  human_factor double precision DEFAULT NULL,
  category int DEFAULT NULL,
  data text NOT NULL
)   ;




create index ix_ticket_history_ticketid on ticket_history (ticketid) ;
create index ix_ticket_history_modified on ticket_history (modified) ;
create index ix_ticket_history_state on ticket_history (state) ;

