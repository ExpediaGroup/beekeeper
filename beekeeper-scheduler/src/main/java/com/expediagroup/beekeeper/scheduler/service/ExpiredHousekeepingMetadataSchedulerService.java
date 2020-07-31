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
package com.expediagroup.beekeeper.scheduler.service;

import static java.lang.String.format;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.monitoring.TimedTaggable;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;

@Service
public class ExpiredHousekeepingMetadataSchedulerService implements SchedulerService {

  private static final Logger log = LoggerFactory.getLogger(ExpiredHousekeepingMetadataSchedulerService.class);
  private static final LifecycleEventType LIFECYCLE_EVENT_TYPE = EXPIRED;

  private final HousekeepingMetadataRepository housekeepingMetadataRepository;

  @Autowired
  public ExpiredHousekeepingMetadataSchedulerService(HousekeepingMetadataRepository housekeepingMetadataRepository) {
    this.housekeepingMetadataRepository = housekeepingMetadataRepository;
  }

  @Override
  public LifecycleEventType getLifecycleEventType() {
    return LIFECYCLE_EVENT_TYPE;
  }

  @Override
  @TimedTaggable("metadata-scheduled")
  public void scheduleForHousekeeping(HousekeepingEntity housekeepingEntity) {
    HousekeepingMetadata housekeepingMetadata = createOrUpdateHousekeepingMetadata(
        (HousekeepingMetadata) housekeepingEntity);
    try {
      housekeepingMetadataRepository.save(housekeepingMetadata);
      log.info(format("Successfully scheduled %s", housekeepingMetadata));
    } catch (Exception e) {
      throw new BeekeeperException(format("Unable to schedule %s", housekeepingMetadata), e);
    }
  }

  private HousekeepingMetadata createOrUpdateHousekeepingMetadata(HousekeepingMetadata housekeepingMetadata) {
    Optional<HousekeepingMetadata> housekeepingMetadataOptional = housekeepingMetadataRepository.findRecordForCleanupByDbTableAndPartitionName(
        housekeepingMetadata.getDatabaseName(), housekeepingMetadata.getTableName(),
        housekeepingMetadata.getPartitionName());

    if (housekeepingMetadataOptional.isEmpty()) {
      return housekeepingMetadata;
    }

    HousekeepingMetadata existingHousekeepingMetadata = housekeepingMetadataOptional.get();
    existingHousekeepingMetadata.setPath(housekeepingMetadata.getPath());
    existingHousekeepingMetadata.setHousekeepingStatus(housekeepingMetadata.getHousekeepingStatus());
    existingHousekeepingMetadata.setCleanupDelay(housekeepingMetadata.getCleanupDelay());
    existingHousekeepingMetadata.setClientId(housekeepingMetadata.getClientId());

    return existingHousekeepingMetadata;
  }
}
