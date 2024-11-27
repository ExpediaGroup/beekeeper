package com.expediagroup.beekeeper.core.service;

import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.core.model.history.BeekeeperHistory;
import com.expediagroup.beekeeper.core.repository.BeekeeperHistoryRepository;

public class BeekeeperHistoryService {

  private static final Logger log = LoggerFactory.getLogger(BeekeeperHistoryService.class);

  private final BeekeeperHistoryRepository beekeeperHistoryRepository;

  public BeekeeperHistoryService(BeekeeperHistoryRepository beekeeperHistoryRepository) {
    this.beekeeperHistoryRepository = beekeeperHistoryRepository;
  }

  public void saveHistory(HousekeepingEntity housekeepingEntity, HousekeepingStatus status) {
    String eventDetails = getEventDetails(housekeepingEntity);

    BeekeeperHistory event = BeekeeperHistory.builder()
        .id(housekeepingEntity.getId())
        .eventTimestamp(LocalDateTime.now())
        .databaseName(housekeepingEntity.getDatabaseName())
        .tableName(housekeepingEntity.getTableName())
        .lifecycleType(housekeepingEntity.getLifecycleType())
        .housekeepingStatus(status.name())
        .eventDetails(eventDetails)
        .build();

    log.info("Saving activity in Beekeeper History table; {}", event);
    beekeeperHistoryRepository.save(event);
  }

  private String getEventDetails(HousekeepingEntity entity) {
    return entity.toString();
  }
}
