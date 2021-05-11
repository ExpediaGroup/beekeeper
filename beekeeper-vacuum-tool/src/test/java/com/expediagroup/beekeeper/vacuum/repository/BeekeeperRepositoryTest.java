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
package com.expediagroup.beekeeper.vacuum.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.vacuum.TestApplication;

@ExtendWith(SpringExtension.class)
@TestPropertySource(properties = {
    "metastore-uri=thrift://localhost:1234",
    "database=test_db",
    "table=test_table",
    "default-cleanup-delay=P1D",
    "dry-run=false",
    "spring.jpa.hibernate.ddl-auto=update",
    "spring.jpa.database=default" })
@ContextConfiguration(classes = { TestApplication.class }, loader = AnnotationConfigContextLoader.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class BeekeeperRepositoryTest {

  @Autowired
  private BeekeeperRepository repository;

  @BeforeEach
  void setUpDb() {
    repository.deleteAll();
  }

  @Test
  void findAllScheduledPaths() {
    HousekeepingPath path = createEntityHousekeepingPath();
    repository.save(path);
    List<HousekeepingPath> paths = repository.findAllScheduledPaths();
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
  void findAllScheduledPathsAfterPathFailedToBeDeleted() {
    HousekeepingPath path = createEntityHousekeepingPath();
    path.setHousekeepingStatus(HousekeepingStatus.FAILED);
    repository.save(path);
    List<HousekeepingPath> paths = repository.findAllScheduledPaths();
    assertThat(paths.size()).isEqualTo(1);
  }

  @Test
  void findAllScheduledPathsAfterPathWasDeleted() {
    HousekeepingPath path = createEntityHousekeepingPath();
    path.setHousekeepingStatus(HousekeepingStatus.DELETED);
    repository.save(path);
    List<HousekeepingPath> paths = repository.findAllScheduledPaths();
    assertThat(paths.size()).isEqualTo(0);
  }

  // we've had issues with null checks being skipped so we have this test to ensure it works from outside beekeeper-core
  @Test
  public void notNullableField() {
    HousekeepingPath path = createEntityHousekeepingPath();
    path.setLifecycleType(null);
    assertThrows(DataIntegrityViolationException.class, () -> repository.save(path));
  }

  private HousekeepingPath createEntityHousekeepingPath() {
    LocalDateTime creationTimestamp = LocalDateTime.now(ZoneId.of("UTC"));
    return HousekeepingPath.builder()
        .path("path")
        .databaseName("database")
        .tableName("table")
        .housekeepingStatus(SCHEDULED)
        .creationTimestamp(creationTimestamp)
        .modifiedTimestamp(creationTimestamp)
        .cleanupDelay(Duration.parse("P3D"))
        .cleanupAttempts(0)
        .lifecycleType(UNREFERENCED.toString())
        .build();
  }
}
