CREATE TABLE location_history (
  clientid bigint NOT NULL,
  location_time timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  location_gps_x float DEFAULT NULL,
  location_gps_y float DEFAULT NULL,
  location_ip varchar(64) DEFAULT NULL
) ;



create index ix_location_history_clientid_location_time on location_history (clientid,location_time) ;
create index ix_location_history_location_gps_x_location_gps_y on location_history (location_gps_x,location_gps_y) ;
create index ix_location_history_location_ip on location_history (location_ip) ;

