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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.monitoring.TimedTaggable;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;

@Service
public class ExpiredHousekeepingMetadataSchedulerService implements SchedulerService {

  private final LifecycleEventType LIFECYCLE_EVENT_TYPE = EXPIRED;
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
    HousekeepingMetadata housekeepingTable = createOrUpdateHousekeepingTable(
        (HousekeepingMetadata) housekeepingEntity);
    try {
      housekeepingMetadataRepository.save(housekeepingTable);
    } catch (Exception e) {
      throw new BeekeeperException(
          format("Unable to schedule table '%s.%s' for deletion", housekeepingTable.getDatabaseName(),
              housekeepingTable.getTableName()), e);
    }
  }

  private HousekeepingMetadata createOrUpdateHousekeepingTable(HousekeepingMetadata housekeepingTable) {
    Optional<HousekeepingMetadata> existingHousekeepingTableOptional = housekeepingMetadataRepository.findRecordForCleanupByDatabaseAndTable(
        housekeepingTable.getDatabaseName(), housekeepingTable.getTableName());

    if (existingHousekeepingTableOptional.isEmpty()) {
      return housekeepingTable;
    }

    HousekeepingMetadata existingHousekeepingTable = existingHousekeepingTableOptional.get();
    existingHousekeepingTable.setHousekeepingStatus(housekeepingTable.getHousekeepingStatus());
    existingHousekeepingTable.setClientId(housekeepingTable.getClientId());

    if (housekeepingTable.getHousekeepingStatus() != HousekeepingStatus.DELETED) {
      existingHousekeepingTable.setCleanupDelay(housekeepingTable.getCleanupDelay());
    }

    return existingHousekeepingTable;
  }
}
