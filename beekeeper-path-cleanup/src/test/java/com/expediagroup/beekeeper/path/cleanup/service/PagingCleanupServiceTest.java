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
package com.expediagroup.beekeeper.path.cleanup.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DELETED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.FAILED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.google.common.collect.Lists;

import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;
import com.expediagroup.beekeeper.path.cleanup.TestApplication;
import com.expediagroup.beekeeper.path.cleanup.handler.UnreferencedPathHandler;

@ExtendWith(SpringExtension.class)
@ExtendWith(MockitoExtension.class)
@TestPropertySource(properties = {
    "hibernate.data-source.driver-class-name=org.h2.Driver",
    "hibernate.dialect=org.hibernate.dialect.H2Dialect",
    "hibernate.hbm2ddl.auto=create",
    "spring.datasource.url=jdbc:h2:mem:beekeeper;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE;MODE=MySQL" })
@ContextConfiguration(classes = { TestApplication.class },
    loader = AnnotationConfigContextLoader.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class PagingCleanupServiceTest {

  private final LocalDateTime localNow = LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
  private PagingPathCleanupService pagingCleanupService;
  private @Captor ArgumentCaptor<HousekeepingPath> pathCaptor;
  private @Autowired HousekeepingPathRepository housekeepingPathRepository;
  private @MockBean PathCleaner pathCleaner;

  @Test
  public void typicalWithPaging() {
    UnreferencedPathHandler handler = new UnreferencedPathHandler(housekeepingPathRepository, pathCleaner);
    pagingCleanupService = new PagingPathCleanupService(List.of(handler), 2, false);

    List<String> paths = List.of("s3://some_foo", "s3://some_bar", "s3://some_foobar");
    paths.forEach(path -> housekeepingPathRepository.save(createEntityHousekeepingPath(path, SCHEDULED)));
    pagingCleanupService.cleanUp(Instant.now());

    verify(pathCleaner, times(3)).cleanupPath(pathCaptor.capture());
    assertThat(pathCaptor.getAllValues())
        .extracting("path")
        .containsExactly(paths.get(0), paths.get(1), paths.get(2));

    housekeepingPathRepository.findAll().forEach(housekeepingPath -> {
      assertThat(housekeepingPath.getCleanupAttempts()).isEqualTo(1);
      assertThat(housekeepingPath.getHousekeepingStatus()).isEqualTo(DELETED);
    });

    pagingCleanupService.cleanUp(Instant.now());
    verifyNoMoreInteractions(pathCleaner);
  }

  @Test
  public void mixOfScheduledAndFailedPaths() {
    UnreferencedPathHandler handler = new UnreferencedPathHandler(housekeepingPathRepository, pathCleaner);
    pagingCleanupService = new PagingPathCleanupService(List.of(handler), 2, false);
    List<HousekeepingPath> paths = List.of(
        createEntityHousekeepingPath("s3://some_foo", SCHEDULED),
        createEntityHousekeepingPath("s3://some_bar", FAILED)
    );
    paths.forEach(path -> housekeepingPathRepository.save(path));
    pagingCleanupService.cleanUp(Instant.now());

    verify(pathCleaner, times(2)).cleanupPath(pathCaptor.capture());
    assertThat(pathCaptor.getAllValues())
        .extracting("path")
        .containsExactly(paths.get(0).getPath(), paths.get(1).getPath());
  }

  @Test
  public void mixOfAllPaths() {
    UnreferencedPathHandler handler = new UnreferencedPathHandler(housekeepingPathRepository, pathCleaner);
    pagingCleanupService = new PagingPathCleanupService(List.of(handler), 2, false);
    List<HousekeepingPath> paths = List.of(
        createEntityHousekeepingPath("s3://some_foo", SCHEDULED),
        createEntityHousekeepingPath("s3://some_bar", FAILED),
        createEntityHousekeepingPath("s3://some_foobar", DELETED)
    );
    paths.forEach(path -> housekeepingPathRepository.save(path));
    pagingCleanupService.cleanUp(Instant.now());

    verify(pathCleaner, times(2)).cleanupPath(pathCaptor.capture());
    assertThat(pathCaptor.getAllValues())
        .extracting("path")
        .containsExactly(paths.get(0).getPath(), paths.get(1).getPath());
  }

  @Test
  void pathCleanerException() {
    UnreferencedPathHandler handler = new UnreferencedPathHandler(housekeepingPathRepository, pathCleaner);
    pagingCleanupService = new PagingPathCleanupService(List.of(handler), 2, false);

    doThrow(new RuntimeException("Error"))
        .doNothing()
        .when(pathCleaner)
        .cleanupPath(any(HousekeepingPath.class));

    List<String> paths = List.of("s3://some_foo", "s3://some_bar");
    paths.forEach(path -> housekeepingPathRepository.save(createEntityHousekeepingPath(path, SCHEDULED)));
    pagingCleanupService.cleanUp(Instant.now());

    verify(pathCleaner, times(2)).cleanupPath(pathCaptor.capture());
    assertThat(pathCaptor.getAllValues())
        .extracting("path")
        .containsExactly(paths.get(0), paths.get(1));

    List<HousekeepingPath> result = Lists.newArrayList(housekeepingPathRepository.findAll());
    assertThat(result.size()).isEqualTo(2);
    HousekeepingPath housekeepingPath1 = result.get(0);
    HousekeepingPath housekeepingPath2 = result.get(1);

    assertThat(housekeepingPath1.getPath()).isEqualTo(paths.get(0));
    assertThat(housekeepingPath1.getHousekeepingStatus()).isEqualTo(FAILED);
    assertThat(housekeepingPath1.getCleanupAttempts()).isEqualTo(1);
    assertThat(housekeepingPath2.getPath()).isEqualTo(paths.get(1));
    assertThat(housekeepingPath2.getHousekeepingStatus()).isEqualTo(DELETED);
    assertThat(housekeepingPath2.getCleanupAttempts()).isEqualTo(1);
  }

  @Test
  @Timeout(value = 10)
  void doNotInfiniteLoopOnRepeatedFailures() {
    UnreferencedPathHandler handler = new UnreferencedPathHandler(housekeepingPathRepository, pathCleaner);
    pagingCleanupService = new PagingPathCleanupService(List.of(handler), 1, false);
    List<HousekeepingPath> paths = List.of(
        createEntityHousekeepingPath("s3://some_foo", FAILED),
        createEntityHousekeepingPath("s3://some_bar", FAILED),
        createEntityHousekeepingPath("s3://some_foobar", FAILED)
    );

    for (int i = 0; i < 5; i++) {
      int finalI = i;
      paths.forEach(path -> {
        if (finalI == 0) {
          housekeepingPathRepository.save(path);
        }

        doThrow(new RuntimeException("Error"))
            .when(pathCleaner)
            .cleanupPath(any());
      });

      pagingCleanupService.cleanUp(Instant.now());
      housekeepingPathRepository.findAll().forEach(path -> {
        assertThat(path.getCleanupAttempts()).isEqualTo(finalI + 1);
        assertThat(path.getHousekeepingStatus()).isEqualTo(FAILED);
      });
    }
  }

  @Test
  @Timeout(value = 10)
  void doNotInfiniteLoopOnDryRunCleanup() {
    UnreferencedPathHandler handler = new UnreferencedPathHandler(housekeepingPathRepository, pathCleaner);
    pagingCleanupService = new PagingPathCleanupService(List.of(handler), 1, true);
    List<HousekeepingPath> paths = List.of(
        createEntityHousekeepingPath("s3://some_foo", SCHEDULED),
        createEntityHousekeepingPath("s3://some_bar", SCHEDULED),
        createEntityHousekeepingPath("s3://some_foobar", SCHEDULED)
    );
    housekeepingPathRepository.saveAll(paths);

    pagingCleanupService.cleanUp(Instant.now());

    housekeepingPathRepository.findAll().forEach(path -> {
      assertThat(path.getCleanupAttempts()).isEqualTo(0);
      assertThat(path.getHousekeepingStatus()).isEqualTo(SCHEDULED);
    });
  }

  private HousekeepingPath createEntityHousekeepingPath(String path, HousekeepingStatus housekeepingStatus) {
    HousekeepingPath housekeepingPath = HousekeepingPath.builder()
        .path(path)
        .databaseName("database")
        .tableName("table")
        .housekeepingStatus(housekeepingStatus)
        .creationTimestamp(localNow)
        .modifiedTimestamp(localNow)
        .modifiedTimestamp(localNow)
        .cleanupDelay(Duration.parse("P3D"))
        .cleanupAttempts(0)
        .lifecycleType(UNREFERENCED.toString())
        .build();
    housekeepingPath.setCleanupTimestamp(localNow);
    return housekeepingPath;
  }
}
