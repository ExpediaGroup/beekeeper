USE beekeeper;
CREATE TABLE IF NOT EXISTS unpartitioned_table_housekeeping (
  id BIGINT(20) AUTO_INCREMENT,
  database_name VARCHAR(512),
  table_name VARCHAR(512),
  housekeeping_status VARCHAR(50) NOT NULL,
  cleanup_delay VARCHAR(50) NOT NULL,
  creation_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  cleanup_timestamp TIMESTAMP NOT NULL,
  cleanup_attempts INT NOT NULL DEFAULT 0,
  client_id VARCHAR(512),
  lifecycle_type varchar(255) NOT NULL,
  PRIMARY KEY (id)
);
