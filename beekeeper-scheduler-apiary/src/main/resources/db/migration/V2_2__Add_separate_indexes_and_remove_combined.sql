USE beekeeper;

DROP INDEX `housekeeping_path_index` ON housekeeping_path;
DROP INDEX `housekeeping_metadata_index` ON housekeeping_metadata;

ALTER TABLE housekeeping_path ADD INDEX `housekeeping_path_index_database_and_table_upper` (upper(database_name), upper(table_name));
ALTER TABLE housekeeping_path ADD INDEX `housekeeping_path_index_database_name_upper` (upper(database_name));
ALTER TABLE housekeeping_path ADD INDEX `housekeeping_path_index_table_name_upper` (upper(table_name));
ALTER TABLE housekeeping_path ADD INDEX `housekeeping_path_index_status` (`housekeeping_status`);

ALTER TABLE housekeeping_metadata ADD INDEX `housekeeping_metadata_index_database_name_upper` (upper(database_name));
ALTER TABLE housekeeping_metadata ADD INDEX `housekeeping_metadata_index_table_name_upper` (upper(table_name));
ALTER TABLE housekeeping_metadata ADD INDEX `housekeeping_metadata_index_status` (`housekeeping_status`);
