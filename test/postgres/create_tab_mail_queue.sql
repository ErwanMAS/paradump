create table mail_queue (
        id             bigint ,
        env_to       varchar(32) DEFAULT NULL,
    env_from     varchar(32) DEFAULT NULL,
    mail_date    timestamptz  NOT NULL DEFAULT CURRENT_TIMESTAMP ,
        subject      varchar(32) DEFAULT NULL,
        part_1_type  varchar(32) DEFAULT NULL,
        part_1_body  text        DEFAULT NULL,
        part_2_type  varchar(32) DEFAULT NULL,
        part_2_body  text        DEFAULT NULL,
        part_3_type  varchar(32) DEFAULT NULL,
        part_3_body  text        DEFAULT NULL,
        part_4_type  varchar(32) DEFAULT NULL,
        part_4_body  text        DEFAULT NULL,
        part_5_type  varchar(32) DEFAULT NULL,
        part_5_body  text        DEFAULT NULL,
    primary    key ( id )
)  ;
