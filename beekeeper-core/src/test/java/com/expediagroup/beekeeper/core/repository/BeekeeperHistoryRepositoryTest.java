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

import static org.assertj.core.api.Assertions.assertThat;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DELETED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.FAILED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.expediagroup.beekeeper.core.TestApplication;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.core.model.PeriodDuration;
import com.expediagroup.beekeeper.core.model.history.BeekeeperHistory;

@ExtendWith(SpringExtension.class)
@TestPropertySource(properties = {
    "hibernate.data-source.driver-class-name=org.h2.Driver",
    "hibernate.dialect=org.hibernate.dialect.H2Dialect",
    "hibernate.hbm2ddl.auto=create",
    "spring.jpa.show-sql=true",
    "spring.datasource.url=jdbc:h2:mem:beekeeper;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE;MODE=MySQL" })
@ContextConfiguration(classes = { TestApplication.class }, loader = AnnotationConfigContextLoader.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class BeekeeperHistoryRepositoryTest {

  protected static final String DATABASE_NAME = "database";
  protected static final String TABLE_NAME = "table";
  protected static final PeriodDuration CLEANUP_DELAY = PeriodDuration.parse("P3D");
  protected static final LocalDateTime COLUMN_TIMESTAMP = LocalDateTime.now(ZoneId.of("UTC"));
  protected static final LocalDateTime EVENT_TIMESTAMP = COLUMN_TIMESTAMP.plus(CLEANUP_DELAY);

  private static final int PAGE = 0;
  private static final int PAGE_SIZE = 500;

  @Autowired
  private BeekeeperHistoryRepository repository;

  @BeforeEach
  public void setupDb() {
    repository.deleteAll();
  }

  @Test
  public void typicalSave() {
    BeekeeperHistory expiredEntry = createExpiredEvent(SCHEDULED);
    BeekeeperHistory unreferencedEntry = createUnreferencedEvent(SCHEDULED);

    repository.save(expiredEntry);
    repository.save(unreferencedEntry);

    List<BeekeeperHistory> historyList = Lists.newArrayList(
        repository.findRecordsByLifecycleType("EXPIRED", PageRequest.of(PAGE, PAGE_SIZE)));
    assertThat(historyList.size()).isEqualTo(1);

    historyList = Lists.newArrayList(
        repository.findRecordsByLifecycleType("UNREFERENCED", PageRequest.of(PAGE, PAGE_SIZE)));
    assertThat(historyList.size()).isEqualTo(1);
  }

  @Test
  public void expired_multipleStatuses() {
    BeekeeperHistory scheduledEntry = createExpiredEvent(SCHEDULED);
    BeekeeperHistory deletedEntry = createExpiredEvent(DELETED);
    BeekeeperHistory failedEntry = createExpiredEvent(FAILED);

    repository.save(scheduledEntry);
    repository.save(deletedEntry);
    repository.save(failedEntry);

    List<BeekeeperHistory> historyList = Lists.newArrayList(
        repository.findRecordsByLifecycleType("EXPIRED", PageRequest.of(PAGE, PAGE_SIZE)));
    assertThat(historyList.size()).isEqualTo(3);
  }

  @Test
  public void unreferenced_multipleStatuses() {
    BeekeeperHistory scheduledEntry = createUnreferencedEvent(SCHEDULED);
    BeekeeperHistory deletedEntry = createUnreferencedEvent(DELETED);
    BeekeeperHistory failedEntry = createUnreferencedEvent(FAILED);

    repository.save(scheduledEntry);
    repository.save(deletedEntry);
    repository.save(failedEntry);

    List<BeekeeperHistory> historyList = Lists.newArrayList(
        repository.findRecordsByLifecycleType("UNREFERENCED", PageRequest.of(PAGE, PAGE_SIZE)));
    assertThat(historyList.size()).isEqualTo(3);
  }

  protected BeekeeperHistory createExpiredEvent(HousekeepingStatus status) {
    HousekeepingMetadata entity = HousekeepingMetadata.builder()
        .cleanupAttempts(3)
        .cleanupDelay(PeriodDuration.parse("P1D"))
        .partitionName("event_date")
        .creationTimestamp(COLUMN_TIMESTAMP)
        .modifiedTimestamp(COLUMN_TIMESTAMP)
        .build();

    return createHistoryEntry("EXPIRED", status, entity.toString());
  }

  protected BeekeeperHistory createUnreferencedEvent(HousekeepingStatus status) {
    HousekeepingPath entity = HousekeepingPath.builder()
        .cleanupAttempts(3)
        .cleanupDelay(PeriodDuration.parse("P1D"))
        .creationTimestamp(COLUMN_TIMESTAMP)
        .modifiedTimestamp(COLUMN_TIMESTAMP)
        .build();

    return createHistoryEntry("UNREFERENCED", status, entity.toString());
  }

  protected BeekeeperHistory createHistoryEntry(String lifecycleType, HousekeepingStatus status,
      String eventDetails) {
    return BeekeeperHistory.builder()
        .eventTimestamp(EVENT_TIMESTAMP)
        .databaseName(DATABASE_NAME)
        .tableName(TABLE_NAME)
        .lifecycleType(lifecycleType)
        .housekeepingStatus(status.name())
        .eventDetails(eventDetails)
        .build();
  }
}
