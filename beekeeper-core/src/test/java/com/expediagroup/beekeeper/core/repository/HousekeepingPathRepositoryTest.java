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
package com.expediagroup.beekeeper.core.repository;

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
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.google.common.collect.Lists;

import com.expediagroup.beekeeper.core.TestApplication;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;

@ExtendWith(SpringExtension.class)
@TestPropertySource(properties = {
    "hibernate.data-source.driver-class-name=org.h2.Driver",
    "hibernate.dialect=org.hibernate.dialect.H2Dialect",
    "hibernate.hbm2ddl.auto=create",
    "spring.jpa.show-sql=true",
    "spring.datasource.url=jdbc:h2:mem:beekeeper;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE;MODE=MySQL" })
@ContextConfiguration(classes = { TestApplication.class }, loader = AnnotationConfigContextLoader.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class HousekeepingPathRepositoryTest {

  private static final LocalDateTime CREATION_TIMESTAMP = LocalDateTime.now(ZoneId.of("UTC"));
  private static final Duration CLEANUP_DELAY = Duration.parse("P3D");
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
    assertThat(savedPath.getCleanupDelay()).isEqualTo(Duration.parse("P3D"));
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
    assertThat(savedPath.getCleanupTimestamp().getHour()).isEqualTo(utcHour);
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

    Page<HousekeepingPath> result = housekeepingPathRepository
        .findRecordsForCleanupByModifiedTimestamp(CLEANUP_TIMESTAMP, PageRequest.of(PAGE, PAGE_SIZE));
    assertThat(result.getContent().get(0).getPath()).isEqualTo("path");
  }

  @Test
  void findRecordsForCleanupByModifiedTimestampZeroResults() {
    HousekeepingPath path = createEntityHousekeepingPath();
    path.setHousekeepingStatus(DELETED);
    housekeepingPathRepository.save(path);

    Page<HousekeepingPath> result = housekeepingPathRepository
        .findRecordsForCleanupByModifiedTimestamp(LocalDateTime.now(), PageRequest.of(PAGE, PAGE_SIZE));
    assertThat(result.getContent().size()).isEqualTo(0);
  }

  @Test
  void findRecordsForCleanupByModifiedTimestampMixedPathStatus() {
    LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));

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

    Page<HousekeepingPath> result = housekeepingPathRepository
        .findRecordsForCleanupByModifiedTimestamp(CLEANUP_TIMESTAMP, PageRequest.of(PAGE, PAGE_SIZE));
    assertThat(result.getContent().size()).isEqualTo(2);
  }

  @Test
  void findRecordsForCleanupByModifiedTimestampRespectsOrder() {
    LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
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

  private HousekeepingPath createEntityHousekeepingPath() {
    return HousekeepingPath.builder()
        .path("path")
        .databaseName("database")
        .tableName("table")
        .housekeepingStatus(SCHEDULED)
        .creationTimestamp(CREATION_TIMESTAMP)
        .modifiedTimestamp(CREATION_TIMESTAMP)
        .cleanupDelay(CLEANUP_DELAY)
        .cleanupAttempts(0)
        .lifecycleType(UNREFERENCED.toString())
        .build();
  }
}
