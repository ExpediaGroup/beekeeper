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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import static com.expediagroup.beekeeper.api.response.MetadataResponseConverter.convertToHousekeepingMetadataResponse;
import static com.expediagroup.beekeeper.api.response.MetadataResponseConverter.convertToHousekeepingMetadataResponsePage;
import static com.expediagroup.beekeeper.api.util.DummyHousekeepingMetadataGenerator.generateDummyHousekeepingMetadata;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;

public class HousekeepingMetadataResponseTest {

  private static final String databaseName = "randomDatabase";
  private static final String tableName = "randomTable";
  private static final String path = "s3://some/path/";
  private static final String partitionName = null;
  private static final HousekeepingStatus housekeepingStatus = SCHEDULED;
  private static final LocalDateTime creationTimestamp = LocalDateTime.now(ZoneId.of("UTC"));
  private static final Duration cleanupDelay = Duration.parse("P3D");
  private static final int cleanupAttempts = 0;
  private static final String lifecycleEventType = EXPIRED.toString();

  @Test
  public void testConvertToHouseKeepingMetadataResponse(){
    HousekeepingMetadata metadata = HousekeepingMetadata.builder()
        .path(path)
        .databaseName(databaseName)
        .tableName(tableName)
        .partitionName(partitionName)
        .housekeepingStatus(housekeepingStatus)
        .creationTimestamp(creationTimestamp)
        .modifiedTimestamp(creationTimestamp)
        .cleanupDelay(cleanupDelay)
        .cleanupAttempts(cleanupAttempts)
        .lifecycleType(lifecycleEventType)
        .build();

    HousekeepingMetadataResponse housekeepingMetadataResponse = convertToHousekeepingMetadataResponse(metadata);

    assertThat(housekeepingMetadataResponse.getPath()).isEqualTo(path);
    assertThat(housekeepingMetadataResponse.getDatabaseName()).isEqualTo(databaseName);
    assertThat(housekeepingMetadataResponse.getTableName()).isEqualTo(tableName);
    assertThat(housekeepingMetadataResponse.getPartitionName()).isEqualTo(partitionName);
    assertThat(housekeepingMetadataResponse.getHousekeepingStatus()).isEqualTo(housekeepingStatus);
    assertThat(housekeepingMetadataResponse.getCreationTimestamp()).isEqualTo(creationTimestamp);
    assertThat(housekeepingMetadataResponse.getModifiedTimestamp()).isEqualTo(creationTimestamp);
    assertThat(housekeepingMetadataResponse.getCleanupDelay()).isEqualTo(cleanupDelay);
    assertThat(housekeepingMetadataResponse.getCleanupAttempts()).isEqualTo(cleanupAttempts);
    assertThat(housekeepingMetadataResponse.getLifecycleType()).isEqualTo(lifecycleEventType);
  }

  @Test
  public void testConvertToHouseKeepingMetadataResponsePage(){
    HousekeepingMetadata metadata1 = generateDummyHousekeepingMetadata("some_database", "some_table");
    HousekeepingMetadata metadata2 = generateDummyHousekeepingMetadata("some_database", "some_table");
    HousekeepingMetadataResponse metadataResponse1 = convertToHousekeepingMetadataResponse(metadata1);
    HousekeepingMetadataResponse metadataResponse2 = convertToHousekeepingMetadataResponse(metadata2);

    Page<HousekeepingMetadata> housekeepingMetadataList = new PageImpl<>(List.of(metadata1, metadata2));
    Page<HousekeepingMetadataResponse> metadataResponsePage = new PageImpl<>(List.of(metadataResponse1, metadataResponse2));

    assertThat(metadataResponsePage).isEqualTo(convertToHousekeepingMetadataResponsePage(housekeepingMetadataList));
  }

}
