/**
 * Copyright (C) 2019-2025 Expedia, Inc.
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

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;

import com.expediagroup.beekeeper.core.model.history.BeekeeperHistory;

public interface BeekeeperHistoryRepository extends PagingAndSortingRepository<BeekeeperHistory, Long>,
    JpaSpecificationExecutor<BeekeeperHistory> {

  @Query(value = "from BeekeeperHistory t where t.lifecycleType = :lifecycle")
  Slice<BeekeeperHistory> findRecordsByLifecycleType(
      @Param("lifecycle") String lifecycle,
      Pageable pageable);
}
