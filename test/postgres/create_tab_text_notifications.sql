CREATE TABLE text_notifications (
        id bigint NOT NULL DEFAULT '0',
    text_date timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expiration_date timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
        phone_country_code varchar(4) DEFAULT NULL,
       phone_number varchar(12) DEFAULT NULL,
    text_body           text,
        PRIMARY KEY (id)
)    ;
