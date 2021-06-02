/**
 * Copyright (C) 2019-2021 Expedia, Inc.
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
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;

public interface HousekeepingMetadataRepository extends PagingAndSortingRepository<HousekeepingMetadata, Long>,
    JpaSpecificationExecutor<HousekeepingMetadata> {

  @Query(value = "from HousekeepingMetadata t where t.cleanupTimestamp <= :instant "
      + "and (t.housekeepingStatus = 'SCHEDULED' or t.housekeepingStatus = 'FAILED') "
      + "and t.modifiedTimestamp <= :instant order by t.modifiedTimestamp")
  Page<HousekeepingMetadata> findRecordsForCleanupByModifiedTimestamp(
      @Param("instant") LocalDateTime instant,
      Pageable pageable);

  /**
   * Returns the record that matches the inputs given, if there is one.
   *
   * @implNote To get the record for a partitioned table both the input value and the value of the partitionName of the current
   * record must be NULL.
   *
   * @param databaseName
   * @param tableName
   * @param partitionName
   * @return
   */
  @Query(value = "from HousekeepingMetadata t "
      + "where t.databaseName = :databaseName "
      + "and t.tableName = :tableName "
      + "and (t.partitionName = :partitionName or (:partitionName is NULL and t.partitionName is NULL)) " // To handle special null case
      + "and (t.housekeepingStatus = 'SCHEDULED' or t.housekeepingStatus = 'FAILED')")
  Optional<HousekeepingMetadata> findRecordForCleanupByDbTableAndPartitionName(
      @Param("databaseName") String databaseName,
      @Param("tableName") String tableName, @Param("partitionName") String partitionName);

  /**
   * Returns the maximum value for the cleanupTimestamp for a database and table name pair.
   *
   * @param databaseName
   * @param tableName
   * @return
   */
  @Query(value = "select max(cleanupTimestamp) from HousekeepingMetadata t "
      + "where t.databaseName = :databaseName "
      + "and t.tableName = :tableName "
      + "and (t.housekeepingStatus = 'SCHEDULED' or t.housekeepingStatus = 'FAILED')")
  LocalDateTime findMaximumCleanupTimestampForDbAndTable(
      @Param("databaseName") String databaseName,
      @Param("tableName") String tableName);

  /**
   * This method returns the count of all records for a database and table name pair where the partitionName is not null.
   *
   * @param databaseName
   * @param tableName
   * @return A count of the number of partitions on this table.
   */
  @Query(value = "select count(partitionName) from HousekeepingMetadata t "
      + "where t.databaseName = :databaseName "
      + "and t.tableName = :tableName "
      + "and (t.housekeepingStatus = 'SCHEDULED' or t.housekeepingStatus = 'FAILED')")
  Long countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(
      @Param("databaseName") String databaseName,
      @Param("tableName") String tableName);

  /**
   * This method is used for dry runs since the entries are not being updated. It counts the number of partitions on a
   * table which have not yet expired, i.e. they will not be cleaned up in this instant.
   *
   * @param instant
   * @param databaseName
   * @param tableName
   * @return A count of the number of existing partitions on this table
   */
  @Query(value = "select count(partitionName) from HousekeepingMetadata t "
      + "where t.databaseName = :databaseName "
      + "and t.tableName = :tableName "
      + "and (t.housekeepingStatus = 'SCHEDULED' or t.housekeepingStatus = 'FAILED') "
      + "and t.cleanupTimestamp >= :instant")
  Long countRecordsForDryRunWherePartitionIsNotNullOrExpired(
      @Param("instant") LocalDateTime instant,
      @Param("databaseName") String databaseName,
      @Param("tableName") String tableName);
}
