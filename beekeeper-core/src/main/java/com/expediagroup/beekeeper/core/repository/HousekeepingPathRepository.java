/**
 * Copyright (C) 2019 Expedia, Inc.
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

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;

import javax.transaction.Transactional;

@Repository
public interface HousekeepingPathRepository extends JpaRepository<EntityHousekeepingPath, Long> {

  @Query(value = "from EntityHousekeepingPath p where p.cleanupTimestamp <= :instant "
      + "and (p.pathStatus = 'SCHEDULED' or p.pathStatus = 'FAILED') "
      + "and p.modifiedTimestamp <= :instant order by p.modifiedTimestamp")
  Page<EntityHousekeepingPath> findRecordsForCleanupByModifiedTimestamp(@Param("instant") LocalDateTime instant,
      Pageable pageable);

//  @Transactional
//  void updateExpiredRows(@Param("databaseName") String databaseName, @Param("tableName") String tableName);

  @Modifying
  @Transactional
  @Query("delete from EntityHousekeepingPath p where "
      + " p.databaseName=:databaseName and "
      + " p.tableName=:tableName and "
      + " p.lifecycleType = 'EXPIRED'")
  void cleanupOldExpiredRows(
          @Param("databaseName") String databaseName,
          @Param("tableName") String tableName);
}
