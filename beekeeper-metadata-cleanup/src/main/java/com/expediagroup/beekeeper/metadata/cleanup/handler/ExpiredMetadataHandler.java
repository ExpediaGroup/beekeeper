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
package com.expediagroup.beekeeper.metadata.cleanup.handler;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.time.LocalDateTime;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import com.expediagroup.beekeeper.core.metadata.MetadataCleaner;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.path.PathCleaner;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;

@Component
public class ExpiredMetadataHandler extends GenericMetadataHandler {

  private HousekeepingMetadataRepository housekeepingMetadataRepository;
  private MetadataCleaner metadataCleaner;
  private PathCleaner pathCleaner;

  @Autowired
  public ExpiredMetadataHandler(
      HousekeepingMetadataRepository housekeepingMetadataRepository,
      @Qualifier("hiveTableCleaner") MetadataCleaner metadataCleaner,
      @Qualifier("s3PathCleaner") PathCleaner pathCleaner) {
    this.housekeepingMetadataRepository = housekeepingMetadataRepository;
    this.metadataCleaner = metadataCleaner;
    this.pathCleaner = pathCleaner;
  }

  @Override
  public HousekeepingMetadataRepository getHousekeepingMetadataRepository() {
    return housekeepingMetadataRepository;
  }

  @Override
  public LifecycleEventType getLifecycleType() {
    return EXPIRED;
  }

  @Override
  public MetadataCleaner getMetadataCleaner() {
    return metadataCleaner;
  }

  @Override
  public PathCleaner getPathCleaner() {
    return pathCleaner;
  }

  @Override
  public Page<HousekeepingMetadata> findRecordsToClean(LocalDateTime instant, Pageable pageable) {
    return housekeepingMetadataRepository.findRecordsForCleanupByModifiedTimestamp(instant, pageable);
  }


  @Override
  public Page<HousekeepingMetadata> findMatchingRecords(
      String databaseName,
      String tableName,
      Pageable pageable) {
    return housekeepingMetadataRepository.findRecordsForGivenDatabaseAndTable(databaseName, tableName, pageable);
  }

}