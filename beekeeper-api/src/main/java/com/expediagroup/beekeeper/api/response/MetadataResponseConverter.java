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

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;

/*
Ideally, this should be done with Spring projections (See https://www.baeldung.com/spring-data-jpa-projections)
but currently projections do not support using Specification (See https://github.com/spring-projects/spring-data-jpa/issues/1378).

Therefore, this is a 'manual' conversion of the HousekeepingMetadata object, which will be used in the API.
*/

public final class MetadataResponseConverter {

  private MetadataResponseConverter(){};

  private static HousekeepingMetadataResponse convertToHousekeepingMetadataResponse(
      HousekeepingMetadata housekeepingMetadata) {
    return HousekeepingMetadataResponse
        .builder()
        .path(housekeepingMetadata.getPath())
        .databaseName(housekeepingMetadata.getDatabaseName())
        .tableName(housekeepingMetadata.getTableName())
        .partitionName(housekeepingMetadata.getPartitionName())
        .housekeepingStatus(housekeepingMetadata.getHousekeepingStatus())
        .creationTimestamp(housekeepingMetadata.getCreationTimestamp())
        .modifiedTimestamp(housekeepingMetadata.getModifiedTimestamp())
        .cleanupTimestamp(housekeepingMetadata.getCleanupTimestamp())
        .cleanupDelay(housekeepingMetadata.getCleanupDelay())
        .cleanupAttempts(housekeepingMetadata.getCleanupAttempts())
        .lifecycleType(housekeepingMetadata.getLifecycleType())
        .build();
  }

  public static Page<HousekeepingMetadataResponse> convertToHousekeepingMetadataResponsePage(
      Page<HousekeepingMetadata> housekeepingMetadataPage) {
    List<HousekeepingMetadata> housekeepingMetadataList = housekeepingMetadataPage.getContent();
    List<HousekeepingMetadataResponse> housekeepingMetadataResponseList = new ArrayList<>();
    for (HousekeepingMetadata housekeepingMetadata : housekeepingMetadataList) {
      HousekeepingMetadataResponse housekeepingMetadataResponse = convertToHousekeepingMetadataResponse(
          housekeepingMetadata);
      housekeepingMetadataResponseList.add(housekeepingMetadataResponse);
    }
    return new PageImpl<>(housekeepingMetadataResponseList);
  }

}
