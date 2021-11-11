USE beekeeper;

ALTER TABLE housekeeping_path ADD INDEX `housekeeping_path_index_table_name_upper` (upper(table_name));
ALTER TABLE housekeeping_path ADD INDEX `housekeeping_path_index_status` (`housekeeping_status`);
DROP INDEX `housekeeping_path_index` ON housekeeping_path;

ALTER TABLE housekeeping_metadata ADD INDEX `housekeeping_metadata_index_table_name_upper` (upper(table_name));
ALTER TABLE housekeeping_metadata ADD INDEX `housekeeping_metadata_index_status` (`housekeeping_status`);
DROP INDEX `housekeeping_metadata_index` ON housekeeping_metadata;
