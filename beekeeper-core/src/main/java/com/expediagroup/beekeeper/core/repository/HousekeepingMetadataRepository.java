/**
 * Copyright (C) 2019-2020 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.expediagroup.beekeeper.core.repository;

import java.time.LocalDateTime;
import java.util.Optional;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;

public interface HousekeepingMetadataRepository extends JpaRepository<HousekeepingMetadata, Long> {

  @Query(value = "from HousekeepingMetadata t where t.cleanupTimestamp <= :instant "
      + "and (t.housekeepingStatus = 'SCHEDULED' or t.housekeepingStatus = 'FAILED') "
      + "and t.modifiedTimestamp <= :instant order by t.modifiedTimestamp")
  Page<HousekeepingMetadata> findRecordsForCleanupByModifiedTimestamp(
      @Param("instant") LocalDateTime instant,
      Pageable pageable);

  @Query(value = "from HousekeepingMetadata t "
      + "where t.databaseName = :databaseName "
      + "and t.tableName = :tableName "
      + "and t.partitionName = :partitionName "
      + "and (t.housekeepingStatus = 'SCHEDULED' or t.housekeepingStatus = 'FAILED')")
  Optional<HousekeepingMetadata> findRecordForCleanupByDatabaseAndTable(@Param("databaseName") String databaseName,
      @Param("tableName") String tableName, @Param("partitionName") String partitionName);
  Optional<HousekeepingMetadata> findRecordForCleanupByDatabaseAndTable(
      @Param("databaseName") String databaseName,
      @Param("tableName") String tableName);

  /**
   * This method returns all the records for a database and table name pair. Each unpartitioned table will have a single
   * entry in the HousekeepingMetadata table. If a table is partitioned there will be multiple entries for it in the
   * HousekeepingMetadata table - one for each partition, and another for the table itself.
   * 
   * @param databaseName
   * @param tableName
   * @return A page of entries from the HouseKeepingMetadata.
   */
  @Query(value = "from HousekeepingMetadata t "
      + "where t.databaseName = :databaseName "
      + "and t.tableName = :tableName "
      + "and (t.housekeepingStatus = 'SCHEDULED' or t.housekeepingStatus = 'FAILED')")
  Page<HousekeepingMetadata> findRecordsForGivenDatabaseAndTable(
      @Param("databaseName") String databaseName,
      @Param("tableName") String tableName,
      Pageable pageable);
}
