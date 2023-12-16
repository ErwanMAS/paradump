CREATE TABLE $DB.client_info (
  id 	     		bigint NOT NULL,
  email    		varchar(255) ,
  status 		smallint NOT NULL,
  insert_ts   datetimeoffset  NOT NULL DEFAULT SYSDATETIMEOFFSET() ,
  update_ts   datetimeoffset  NOT NULL DEFAULT SYSDATETIMEOFFSET() ,
  PRIMARY KEY (id)
) 

