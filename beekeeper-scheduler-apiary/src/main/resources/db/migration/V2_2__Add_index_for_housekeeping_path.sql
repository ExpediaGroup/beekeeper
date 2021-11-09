USE beekeeper;

ALTER TABLE housekeeping_path ADD INDEX `housekeeping_path_index` (`housekeeping_status`, `database_name`(300), `table_name`(300));
ALTER TABLE housekeeping_metadata ADD INDEX `housekeeping_metadata_index` (`housekeeping_status`, `database_name`(300), `table_name`(300));
