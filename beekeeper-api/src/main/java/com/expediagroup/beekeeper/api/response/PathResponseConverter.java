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
package com.expediagroup.beekeeper.api.response;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;

import com.expediagroup.beekeeper.core.model.HousekeepingPath;

/*
Ideally, this should be done with Spring projections (See https://www.baeldung.com/spring-data-jpa-projections)
but currently projections do not support using Specification (See https://github.com/spring-projects/spring-data-jpa/issues/1378).

Therefore, this is a 'manual' conversion of the HousekeepingPath object, which will be used in the API.
*/

public class PathResponseConverter {

  private static HousekeepingPathResponse convertToHousekeepingPathResponse(
      HousekeepingPath housekeepingPath) {
    return HousekeepingPathResponse
        .builder()
        .path(housekeepingPath.getPath())
        .databaseName(housekeepingPath.getDatabaseName())
        .tableName(housekeepingPath.getTableName())
        .housekeepingStatus(housekeepingPath.getHousekeepingStatus())
        .creationTimestamp(housekeepingPath.getCreationTimestamp())
        .modifiedTimestamp(housekeepingPath.getModifiedTimestamp())
        .cleanupTimestamp(housekeepingPath.getCleanupTimestamp())
        .cleanupDelay(housekeepingPath.getCleanupDelay())
        .cleanupAttempts(housekeepingPath.getCleanupAttempts())
        .lifecycleType(housekeepingPath.getLifecycleType())
        .build();
  }

  public static Page<HousekeepingPathResponse> convertToHousekeepingPathResponsePage(
      Page<HousekeepingPath> housekeepingPathPage) {
    List<HousekeepingPath> housekeepingPathList = housekeepingPathPage.getContent();
    List<HousekeepingPathResponse> housekeepingPathResponseList = new ArrayList<>();
    for (HousekeepingPath housekeepingPath : housekeepingPathList) {
      HousekeepingPathResponse housekeepingPathResponse = convertToHousekeepingPathResponse(
          housekeepingPath);
      housekeepingPathResponseList.add(housekeepingPathResponse);
    }
    return new PageImpl<>(housekeepingPathResponseList);
  }

}