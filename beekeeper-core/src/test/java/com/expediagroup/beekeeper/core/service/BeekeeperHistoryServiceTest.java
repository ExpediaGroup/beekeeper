package com.expediagroup.beekeeper.core.service;

import static org.mockito.Mockito.verify;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DELETED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.core.JsonProcessingException;

import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.PeriodDuration;
import com.expediagroup.beekeeper.core.model.history.BeekeeperHistory;
import com.expediagroup.beekeeper.core.repository.BeekeeperHistoryRepository;

@ExtendWith(MockitoExtension.class)
public class BeekeeperHistoryServiceTest {

  private BeekeeperHistoryService beekeeperHistoryService;

  private @Mock BeekeeperHistoryRepository repository;

  private static final String DATABASE = "database";
  private static final String TABLE_NAME = "tableName";
  private static final String VALID_TABLE_PATH = "s3://bucket/table";
  private static final String VALID_PARTITION_PATH = "s3://bucket/table/partition";
  private static final String PARTITION_NAME = "event_date=2020-01-01/event_hour=0/event_type=A";
  private static final LocalDateTime CLEANUP_INSTANCE = LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
  private static final PeriodDuration CLEANUP_DELAY = PeriodDuration.parse("P3D");

  @BeforeEach
  public void setup() {
    beekeeperHistoryService = new BeekeeperHistoryService(repository);
  }

  @Test
  void expiredHistory() throws JsonProcessingException {
    HousekeepingMetadata metadata = createHousekeepingMetadata();
    String details = createEventDetails(metadata);
    BeekeeperHistory history = createHistoryEvent(metadata, details, "DELETED");

    beekeeperHistoryService.saveHistory(metadata, DELETED);
    verify(repository).save(history);
  }

  @Test
  void unreferencedHistory() throws JsonProcessingException {
    HousekeepingPath path = createHousekeepingPath();
    String details = createEventDetails(path);
    BeekeeperHistory history = createHistoryEvent(path, details, "SCHEDULED");

    beekeeperHistoryService.saveHistory(path, SCHEDULED);
    verify(repository).save(history);
  }

  private BeekeeperHistory createHistoryEvent(HousekeepingEntity entity, String eventDetails, String status) {
    return BeekeeperHistory.builder()
        .id(entity.getId())
        .databaseName(entity.getDatabaseName())
        .tableName(entity.getTableName())
        .lifecycleType(entity.getLifecycleType())
        .housekeepingStatus(status)
        .eventDetails(eventDetails)
        .build();
  }

  private HousekeepingMetadata createHousekeepingMetadata() {
    return HousekeepingMetadata.builder()
        .path(VALID_TABLE_PATH)
        .databaseName(DATABASE)
        .tableName(TABLE_NAME)
        .partitionName(PARTITION_NAME)
        .housekeepingStatus(SCHEDULED)
        .creationTimestamp(CLEANUP_INSTANCE)
        .cleanupDelay(CLEANUP_DELAY)
        .cleanupAttempts(0)
        .lifecycleType(EXPIRED.name())
        .build();
  }

  private HousekeepingPath createHousekeepingPath() {
    return HousekeepingPath.builder()
        .path(VALID_PARTITION_PATH)
        .databaseName(DATABASE)
        .tableName(TABLE_NAME)
        .housekeepingStatus(SCHEDULED)
        .creationTimestamp(CLEANUP_INSTANCE)
        .cleanupDelay(CLEANUP_DELAY)
        .cleanupAttempts(0)
        .lifecycleType(UNREFERENCED.name())
        .build();
  }

  private String createEventDetails(HousekeepingEntity housekeepingEntity) {
    return housekeepingEntity.toString();
  }
}
