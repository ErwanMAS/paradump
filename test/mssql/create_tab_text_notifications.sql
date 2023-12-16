CREATE TABLE $DB.text_notifications (
        id bigint NOT NULL DEFAULT '0',
    text_date datetimeoffset  NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    expiration_date datetimeoffset  NOT NULL DEFAULT SYSDATETIMEOFFSET(),
        phone_country_code varchar(4) DEFAULT NULL,
       phone_number varchar(12) DEFAULT NULL,
    text_body           text,
        PRIMARY KEY (id)
)    ;
