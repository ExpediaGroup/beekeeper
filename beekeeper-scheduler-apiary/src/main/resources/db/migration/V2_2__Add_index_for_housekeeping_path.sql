USE beekeeper;

ALTER TABLE housekeeping_path ADD INDEX `table_index` (`housekeeping_status`, `database_name`(300), `table_name`(300));
