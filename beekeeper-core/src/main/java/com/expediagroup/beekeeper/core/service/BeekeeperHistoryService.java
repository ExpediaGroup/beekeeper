package com.expediagroup.beekeeper.core.service;

import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.model.history.BeekeeperHistory;
import com.expediagroup.beekeeper.core.repository.BeekeeperHistoryRepository;

@Component
public class BeekeeperHistoryService {

  private static final Logger log = LoggerFactory.getLogger(BeekeeperHistoryService.class);

  private final BeekeeperHistoryRepository beekeeperHistoryRepository;

  @Autowired
  public BeekeeperHistoryService(BeekeeperHistoryRepository beekeeperHistoryRepository) {
    this.beekeeperHistoryRepository = beekeeperHistoryRepository;
  }

  public void saveHistory(HousekeepingEntity housekeepingEntity, String eventDetails, String status) {
    BeekeeperHistory event = BeekeeperHistory.builder()
        .id(housekeepingEntity.getId())
        .eventTimestamp(LocalDateTime.now())
        .databaseName(housekeepingEntity.getDatabaseName())
        .tableName(housekeepingEntity.getTableName())
        .lifecycleType(housekeepingEntity.getLifecycleType())
        .housekeepingStatus(status)
        .eventDetails(eventDetails)
        .build();

    log.info("Saving activity in Beekeeper History table; {}", event);
    beekeeperHistoryRepository.save(event);
  }
}
