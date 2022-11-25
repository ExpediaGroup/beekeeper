/**
 * Copyright (C) 2019-2022 Expedia, Inc.
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

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MONTHS;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertThrows;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DELETED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.FAILED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.collect.Lists;

import com.expediagroup.beekeeper.core.TestApplication;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;

@ExtendWith(SpringExtension.class)
@TestPropertySource(properties = {
    "hibernate.data-source.driver-class-name=org.h2.Driver",
    "hibernate.dialect=org.hibernate.dialect.H2Dialect",
    "hibernate.hbm2ddl.auto=create",
    "spring.jpa.show-sql=true",
    "spring.datasource.url=jdbc:h2:mem:beekeeper;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE;MODE=MySQL" })
@ContextConfiguration(classes = { TestApplication.class }, loader = AnnotationConfigContextLoader.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class HousekeepingPathRepositoryTest {

  private static final LocalDateTime CREATION_TIMESTAMP = LocalDateTime.now(ZoneId.of("UTC"));
  private static final Duration CLEANUP_DELAY = Duration.parse("PT1H");
  private static final LocalDateTime CLEANUP_TIMESTAMP = CREATION_TIMESTAMP.plus(CLEANUP_DELAY);

  private static final int PAGE = 0;
  private static final int PAGE_SIZE = 500;

  @Autowired
  private HousekeepingPathRepository housekeepingPathRepository;

  @BeforeEach
  public void setupDb() {
    housekeepingPathRepository.deleteAll();
  }

  @Test
  public void typicalSave() {
    HousekeepingPath path = createEntityHousekeepingPath();

    housekeepingPathRepository.save(path);

    List<HousekeepingPath> paths = Lists.newArrayList(housekeepingPathRepository.findAll());
    assertThat(paths.size()).isEqualTo(1);
    HousekeepingPath savedPath = paths.get(0);
    assertThat(savedPath.getPath()).isEqualTo("path");
    assertThat(savedPath.getDatabaseName()).isEqualTo("database");
    assertThat(savedPath.getTableName()).isEqualTo("table");
    assertThat(savedPath.getHousekeepingStatus()).isEqualTo(SCHEDULED);
    assertThat(savedPath.getCleanupDelay()).isEqualTo(Duration.parse("PT1H"));
    assertThat(savedPath.getCreationTimestamp()).isNotNull();
    assertThat(savedPath.getModifiedTimestamp()).isNotNull();
    assertThat(savedPath.getModifiedTimestamp()).isNotEqualTo(savedPath.getCreationTimestamp());
    assertThat(savedPath.getCleanupTimestamp()).isEqualTo(path.getCreationTimestamp().plus(path.getCleanupDelay()));
    assertThat(savedPath.getCleanupAttempts()).isEqualTo(0);
  }

  @Test
  public void typicalUpdate() {
    HousekeepingPath path = createEntityHousekeepingPath();
    HousekeepingPath savedPath = housekeepingPathRepository.save(path);

    savedPath.setHousekeepingStatus(DELETED);
    savedPath.setCleanupAttempts(savedPath.getCleanupAttempts() + 1);
    housekeepingPathRepository.save(savedPath);

    List<HousekeepingPath> paths = Lists.newArrayList(housekeepingPathRepository.findAll());
    assertThat(paths.size()).isEqualTo(1);
    HousekeepingPath updatedPath = paths.get(0);
    assertThat(updatedPath.getHousekeepingStatus()).isEqualTo(DELETED);
    assertThat(updatedPath.getCleanupAttempts()).isEqualTo(1);
    assertThat(updatedPath.getModifiedTimestamp()).isNotEqualTo(savedPath.getModifiedTimestamp());
  }

  @Test
  public void notNullableField() {
    HousekeepingPath path = createEntityHousekeepingPath();
    path.setLifecycleType(null);
    assertThrows(DataIntegrityViolationException.class, () -> housekeepingPathRepository.save(path));
  }

  @Test
  void timezone() {
    HousekeepingPath path = createEntityHousekeepingPath();
    housekeepingPathRepository.save(path);
    List<HousekeepingPath> paths = Lists.newArrayList(housekeepingPathRepository.findAll());
    assertThat(paths.size()).isEqualTo(1);

    HousekeepingPath savedPath = paths.get(0);
    int utcHour = LocalDateTime.now(ZoneId.of("UTC")).getHour();
    assertThat(savedPath.getCleanupTimestamp().getHour()).isEqualTo(utcHour + 1);
    assertThat(savedPath.getModifiedTimestamp().getHour()).isEqualTo(utcHour);
    assertThat(savedPath.getCreationTimestamp().getHour()).isEqualTo(utcHour);
  }

  @Test
  public void checkDuplicatePathThrowsException() {
    HousekeepingPath path1 = createEntityHousekeepingPath();
    HousekeepingPath path2 = createEntityHousekeepingPath();
    housekeepingPathRepository.save(path1);
    assertThatExceptionOfType(DataIntegrityViolationException.class)
        .isThrownBy(() -> housekeepingPathRepository.save(path2));
  }

  @Test
  void findRecordsForCleanupByModifiedTimestamp() {
    HousekeepingPath path = createEntityHousekeepingPath();
    housekeepingPathRepository.save(path);

    Slice<HousekeepingPath> result = housekeepingPathRepository
        .findRecordsForCleanupByModifiedTimestamp(CLEANUP_TIMESTAMP, PageRequest.of(PAGE, PAGE_SIZE));
    assertThat(result.getContent().get(0).getPath()).isEqualTo("path");
  }

  @Test
  void findRecordsForCleanupByModifiedTimestampZeroResults() {
    HousekeepingPath path = createEntityHousekeepingPath();
    path.setHousekeepingStatus(DELETED);
    housekeepingPathRepository.save(path);

    Slice<HousekeepingPath> result = housekeepingPathRepository
        .findRecordsForCleanupByModifiedTimestamp(LocalDateTime.now(), PageRequest.of(PAGE, PAGE_SIZE));
    assertThat(result.getContent().size()).isEqualTo(0);
  }

  @Test
  void findRecordsForCleanupByModifiedTimestampMixedPathStatus() {
    HousekeepingPath housekeepingPath1 = createEntityHousekeepingPath();
    housekeepingPathRepository.save(housekeepingPath1);

    HousekeepingPath housekeepingPath2 = createEntityHousekeepingPath();
    housekeepingPath2.setHousekeepingStatus(FAILED);
    housekeepingPath2.setPath("path2");
    housekeepingPathRepository.save(housekeepingPath2);

    HousekeepingPath housekeepingPath3 = createEntityHousekeepingPath();
    housekeepingPath3.setHousekeepingStatus(DELETED);
    housekeepingPath3.setPath("path3");
    housekeepingPathRepository.save(housekeepingPath3);

    Slice<HousekeepingPath> result = housekeepingPathRepository
        .findRecordsForCleanupByModifiedTimestamp(CLEANUP_TIMESTAMP, PageRequest.of(PAGE, PAGE_SIZE));
    assertThat(result.getContent().size()).isEqualTo(2);
  }

  @Test
  void findRecordsForCleanupByModifiedTimestampRespectsOrder() {
    String path1 = "path1";
    String path2 = "path2";

    HousekeepingPath housekeepingPath1 = createEntityHousekeepingPath();
    housekeepingPath1.setPath(path1);
    housekeepingPathRepository.save(housekeepingPath1);

    HousekeepingPath housekeepingPath2 = createEntityHousekeepingPath();
    housekeepingPath2.setPath(path2);
    housekeepingPathRepository.save(housekeepingPath2);

    List<HousekeepingPath> result = housekeepingPathRepository
        .findRecordsForCleanupByModifiedTimestamp(CLEANUP_TIMESTAMP, PageRequest.of(PAGE, PAGE_SIZE))
        .getContent();
    assertThat(result.get(0).getPath()).isEqualTo(path1);
    assertThat(result.get(1).getPath()).isEqualTo(path2);
  }

  @Test
  @Transactional
  public void cleanUpOldDeletedRecords() {
    HousekeepingPath path = createEntityHousekeepingPath("path", CLEANUP_TIMESTAMP.minus(1, MONTHS), DELETED);
    housekeepingPathRepository.save(path);

    housekeepingPathRepository.cleanUpOldDeletedRecords(CLEANUP_TIMESTAMP);
    List<HousekeepingPath> remainingPaths = Lists.newArrayList(housekeepingPathRepository.findAll());
    assertThat(remainingPaths.size()).isEqualTo(0);
  }

  @Test
  @Transactional
  public void cleanUpOldDeletedRecordsNothingToDelete() {
    HousekeepingPath newScheduledPath = createEntityHousekeepingPath("path", CLEANUP_TIMESTAMP, SCHEDULED);
    housekeepingPathRepository.save(newScheduledPath);

    housekeepingPathRepository.cleanUpOldDeletedRecords(CLEANUP_TIMESTAMP.plus(1, DAYS));
    List<HousekeepingPath> remainingPaths = Lists.newArrayList(housekeepingPathRepository.findAll());
    assertThat(remainingPaths.size()).isEqualTo(1);
    assertThat(remainingPaths.get(0)).isEqualTo(newScheduledPath);
  }

  @Test
  @Transactional
  public void cleanUpOldDeletedRecordsMultipleRecords() {
    HousekeepingPath oldDeletedPath = createEntityHousekeepingPath("path1", CLEANUP_TIMESTAMP.minus(2, DAYS), DELETED);
    housekeepingPathRepository.save(oldDeletedPath);
    HousekeepingPath oldDeletedPath1 = createEntityHousekeepingPath("path11", CLEANUP_TIMESTAMP.minus(2, HOURS),
        DELETED);
    housekeepingPathRepository.save(oldDeletedPath1);
    HousekeepingPath oldScheduledPath = createEntityHousekeepingPath("path2", CLEANUP_TIMESTAMP.minus(2, DAYS),
        SCHEDULED);
    housekeepingPathRepository.save(oldScheduledPath);
    HousekeepingPath newDeletedPath = createEntityHousekeepingPath("path3", CREATION_TIMESTAMP, DELETED);
    housekeepingPathRepository.save(newDeletedPath);
    HousekeepingPath newScheduledPath = createEntityHousekeepingPath("path4", CREATION_TIMESTAMP, SCHEDULED);
    housekeepingPathRepository.save(newScheduledPath);

    housekeepingPathRepository.cleanUpOldDeletedRecords(CLEANUP_TIMESTAMP);
    List<HousekeepingPath> remainingPaths = Lists.newArrayList(housekeepingPathRepository.findAll());
    assertThat(remainingPaths.size()).isEqualTo(3);
    assertThat(remainingPaths.get(0)).isEqualTo(oldScheduledPath);
    assertThat(remainingPaths.get(1)).isEqualTo(newDeletedPath);
    assertThat(remainingPaths.get(2)).isEqualTo(newScheduledPath);
  }

  private HousekeepingPath createEntityHousekeepingPath() {
    return createEntityHousekeepingPath("path", CREATION_TIMESTAMP, SCHEDULED);
  }

  private HousekeepingPath createEntityHousekeepingPath(String path, LocalDateTime creationDate,
      HousekeepingStatus status) {
    return HousekeepingPath.builder()
        .path(path)
        .databaseName("database")
        .tableName("table")
        .housekeepingStatus(status)
        .creationTimestamp(creationDate)
        .modifiedTimestamp(creationDate)
        .cleanupDelay(CLEANUP_DELAY)
        .cleanupAttempts(0)
        .lifecycleType(UNREFERENCED.toString())
        .build();
  }
}
