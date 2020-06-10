package com.expediagroup.beekeeper.core.repository;

import java.time.LocalDateTime;
import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.expediagroup.beekeeper.core.model.EntityHousekeepingTable;

public interface HousekeepingTableRepository extends JpaRepository<EntityHousekeepingTable, Long> {

  @Query(value = "from EntityHouseKeepingTable t where t.cleanupTimestamp <= :instant "
      + "and (t.housekeepingStatus = 'SCHEDULED' or t.housekeepingStatus = 'FAILED') "
      + "and t.modifiedTimestamp <= :instant order by t.modifiedTimestamp")
  Page<EntityHousekeepingTable> findRecordsForCleanupByModifiedTimestamp(@Param("instant") LocalDateTime instant,
      Pageable pageable);

  @Query(value = "from EntityHouseKeepingTable t where t.databaseName = :databaseName "
      + "and t.tableName = :tableName "
      + "and (t.housekeepingStatus = 'SCHEDULED' or t.housekeepingStatus = 'FAILED')")
  List<EntityHousekeepingTable> findRecordsForDatabaseAndTable(@Param("databaseName") String databaseName,
      @Param("tableName") String tableName, Pageable pageable);
}
