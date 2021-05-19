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

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.expediagroup.beekeeper.core.model.HousekeepingPath;

@Repository
public interface HousekeepingPathRepository extends PagingAndSortingRepository<HousekeepingPath, Long>
    , JpaSpecificationExecutor<HousekeepingPath> {

  @Query(value = "from HousekeepingPath p where p.cleanupTimestamp <= :instant "
      + "and (p.housekeepingStatus = 'SCHEDULED' or p.housekeepingStatus = 'FAILED') "
      + "and p.modifiedTimestamp <= :instant order by p.modifiedTimestamp")
  Page<HousekeepingPath> findRecordsForCleanupByModifiedTimestamp(@Param("instant") LocalDateTime instant,
      Pageable pageable);

  Page<HousekeepingPath> findAllByDatabaseNameAndTableName(String databaseName, String tableName,
      Pageable pageable);
}
