CREATE TABLE item (
  file_name     varchar(100) NOT NULL,
  user_guid     varchar(50) NOT NULL,
  media_type    varchar(16) NOT NULL,
  date_created  datetime NOT NULL,
  date_updated  timestamp AUTO_INCREMENT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(file_name)
)