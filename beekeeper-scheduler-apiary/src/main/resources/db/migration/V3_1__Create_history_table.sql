USE beekeeper;

CREATE TABLE IF NOT EXISTS beekeeper_history (
  id BIGINT(20) AUTO_INCREMENT,
  event_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  database_name VARCHAR(512),
  table_name VARCHAR(512),
  lifecycle_type VARCHAR(255) NOT NULL,
  housekeeping_status VARCHAR(50) NOT NULL,
  event_details TEXT,
  PRIMARY KEY (id)
);

ALTER TABLE beekeeper_history ADD INDEX `beekeeper_history_index_table_name_upper` ((upper(table_name)));
ALTER TABLE beekeeper_history ADD INDEX `beekeeper_history_index_status` (`housekeeping_status`);
