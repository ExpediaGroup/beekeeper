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
package com.expediagroup.beekeeper.metadata.cleanup.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DELETED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.FAILED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

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

import com.expediagroup.beekeeper.core.metadata.MetadataCleaner;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.core.path.PathCleaner;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;
import com.expediagroup.beekeeper.metadata.cleanup.TestApplication;
import com.expediagroup.beekeeper.metadata.cleanup.handler.ExpiredMetadataHandler;

@ExtendWith(SpringExtension.class)
@ExtendWith(MockitoExtension.class)
@TestPropertySource(properties = {
    "hibernate.data-source.driver-class-name=org.h2.Driver",
    "hibernate.dialect=org.hibernate.dialect.H2Dialect",
    "hibernate.hbm2ddl.auto=create",
    "spring.datasource.url=jdbc:h2:mem:beekeeper;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE;MODE=MySQL" })
@ContextConfiguration(classes = { TestApplication.class }, loader = AnnotationConfigContextLoader.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class PagingMetadataCleanupServiceTest {

  private final LocalDateTime localNow = LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
  private PagingMetadataCleanupService pagingCleanupService;
  private @Captor ArgumentCaptor<HousekeepingMetadata> metadataCaptor;
  private @Captor ArgumentCaptor<HousekeepingPath> pathCaptor;
  private @Autowired HousekeepingMetadataRepository metadataRepository;
  private @MockBean MetadataCleaner metadataCleaner;
  private @MockBean PathCleaner pathCleaner;

  @Test
  public void typical() {
    ExpiredMetadataHandler handler = new ExpiredMetadataHandler(metadataRepository, metadataCleaner, pathCleaner);
    pagingCleanupService = new PagingMetadataCleanupService(List.of(handler), 2, false);

    // TODO
    // wait for vedant to add 'path' to housekeepingMetadata
    // paths.forEach(path -> metadataRepository.save(createHousekeepingMetadata(path, SCHEDULED)));
    List<String> paths = List.of("s3://some_foo", "s3://some_bar", "s3://some_foobar");
    List<String> tables = List.of("table1", "table2", "table3");
    tables.forEach(table -> metadataRepository.save(createHousekeepingMetadata(table, SCHEDULED)));
    pagingCleanupService.cleanUp(Instant.now());

    // expect that the metadata cleaner will be called for each of the tables in the list
    // expect that the path cleaner will be called for each of the paths in the list
    verify(metadataCleaner, times(3)).cleanupMetadata(metadataCaptor.capture());
    assertThat(metadataCaptor.getAllValues())
        .extracting("tableName")
        .containsExactly(tables.get(0), tables.get(1), tables.get(2));

    // TODO
    // uncomment when path is added to housekeepingMetadata
    // verify(pathCleaner, times(3)).cleanupPath(pathCaptor.capture());
    // assertThat(pathCaptor.getAllValues()).extracting("path").containsExactly(paths.get(0), paths.get(1),
    // paths.get(2));

    metadataRepository.findAll().forEach(housekeepingMetadata -> {
      assertThat(housekeepingMetadata.getCleanupAttempts()).isEqualTo(1);
      assertThat(housekeepingMetadata.getHousekeepingStatus()).isEqualTo(DELETED);
    });

    pagingCleanupService.cleanUp(Instant.now());
    verifyNoMoreInteractions(pathCleaner);
  }

  @Test
  public void mixOfScheduledAndFailedPaths() {
    ExpiredMetadataHandler handler = new ExpiredMetadataHandler(metadataRepository, metadataCleaner, pathCleaner);
    pagingCleanupService = new PagingMetadataCleanupService(List.of(handler), 2, false);
    List<HousekeepingMetadata> tables = List
        .of(createHousekeepingMetadata("table1", SCHEDULED), createHousekeepingMetadata("table2", FAILED));
    tables.forEach(table -> metadataRepository.save(table));
    // TODO
    // List<HousekeepingPath> paths = List
    // .of(createEntityHousekeepingPath("s3://some_foo", SCHEDULED),
    // createEntityHousekeepingPath("s3://some_bar", FAILED));
    // paths.forEach(path -> housekeepingPathRepository.save(path));
    pagingCleanupService.cleanUp(Instant.now());

    verify(metadataCleaner, times(2)).cleanupMetadata(metadataCaptor.capture());
    assertThat(metadataCaptor.getAllValues())
        .extracting("tableName")
        .containsExactly(tables.get(0).getTableName(), tables.get(1).getTableName());
    // TODO
    // verify(pathCleaner, times(2)).cleanupPath(pathCaptor.capture());
    // assertThat(pathCaptor.getAllValues())
    // .extracting("path")
    // .containsExactly(paths.get(0).getPath, paths.get(1).getPath);
  }

  // make sure only lifecyles which arent 'deleted' are attempted for cleanup
  @Test
  public void mixOfAllPaths() {
    ExpiredMetadataHandler handler = new ExpiredMetadataHandler(metadataRepository, metadataCleaner, pathCleaner);
    pagingCleanupService = new PagingMetadataCleanupService(List.of(handler), 2, false);
    List<HousekeepingMetadata> tables = List
        .of(createHousekeepingMetadata("table1", SCHEDULED), createHousekeepingMetadata("table2", FAILED),
            createHousekeepingMetadata("table3", DELETED));
    // TODO
    // List<HousekeepingMetadata> paths = List
    // .of(createHousekeepingMetadata("s3://some_foo", SCHEDULED), createHousekeepingMetadata("s3://some_bar", FAILED),
    // createHousekeepingMetadata("s3://some_foobar", DELETED));
    tables.forEach(path -> metadataRepository.save(path));
    pagingCleanupService.cleanUp(Instant.now());

    verify(metadataCleaner, times(2)).cleanupMetadata(metadataCaptor.capture());
    assertThat(metadataCaptor.getAllValues())
        .extracting("tableName")
        .containsExactly(tables.get(0).getTableName(), tables.get(1).getTableName());
    // TODO
    // verify(pathCleaner, times(2)).cleanupPath(pathCaptor.capture());
    // assertThat(pathCaptor.getAllValues())
    // .extracting("path")
    // .containsExactly(paths.get(0).getPath(), paths.get(1).getPath());
  }

  @Test
  void metadataCleanerException() {
    ExpiredMetadataHandler handler = new ExpiredMetadataHandler(metadataRepository, metadataCleaner, pathCleaner);
    pagingCleanupService = new PagingMetadataCleanupService(List.of(handler), 2, false);

    doThrow(new RuntimeException("Error"))
        .doNothing()
        .when(metadataCleaner)
        .cleanupMetadata(any(HousekeepingMetadata.class));

    List<String> tables = List.of("s3://some_foo", "s3://some_bar");
    tables.forEach(table -> metadataRepository.save(createHousekeepingMetadata(table, SCHEDULED)));
    // TODO
    // List<String> paths = List.of("s3://some_foo", "s3://some_bar");
    // paths.forEach(path -> metadataRepository.save(createHousekeepingMetadata(path, SCHEDULED)));
    pagingCleanupService.cleanUp(Instant.now());

    verify(metadataCleaner, times(2)).cleanupMetadata(metadataCaptor.capture());
    assertThat(metadataCaptor.getAllValues()).extracting("tableName").containsExactly(tables.get(0), tables.get(1));

    List<HousekeepingMetadata> result = metadataRepository.findAll();
    assertThat(result.size()).isEqualTo(2);
    HousekeepingMetadata housekeepingMetadata1 = result.get(0);
    HousekeepingMetadata housekeepingMetadata2 = result.get(1);

    
    assertThat(housekeepingMetadata1.getTableName()).isEqualTo(tables.get(0));
//    assertThat(housekeepingMetadata1.getPath()).isEqualTo(tables.get(0));
    assertThat(housekeepingMetadata1.getHousekeepingStatus()).isEqualTo(FAILED);
    assertThat(housekeepingMetadata1.getCleanupAttempts()).isEqualTo(1);
    assertThat(housekeepingMetadata2.getTableName()).isEqualTo(tables.get(1));
//    assertThat(housekeepingMetadata2.getPath()).isEqualTo(tables.get(1));
    assertThat(housekeepingMetadata2.getHousekeepingStatus()).isEqualTo(DELETED);
    assertThat(housekeepingMetadata2.getCleanupAttempts()).isEqualTo(1);
  }

  @Test
  @Timeout(value = 10)
  void doNotInfiniteLoopOnRepeatedFailures() {
    ExpiredMetadataHandler handler = new ExpiredMetadataHandler(metadataRepository, metadataCleaner, pathCleaner);
    pagingCleanupService = new PagingMetadataCleanupService(List.of(handler), 2, false);

    List<HousekeepingMetadata> tables = List
        .of(createHousekeepingMetadata("table1", FAILED), createHousekeepingMetadata("table2", FAILED),
            createHousekeepingMetadata("table3", FAILED));
    // TODO
    // List<HousekeepingPath> paths = List
    // .of(createEntityHousekeepingPath("s3://some_foo", FAILED),
    // createEntityHousekeepingPath("s3://some_bar", FAILED),
    // createEntityHousekeepingPath("s3://some_foobar", FAILED));

    for (int i = 0; i < 5; i++) {
      int finalI = i;
      tables.forEach(path -> {
        if (finalI == 0) {
          metadataRepository.save(path);
        }

        doThrow(new RuntimeException("Error")).when(metadataCleaner).cleanupMetadata(any());
      });

      pagingCleanupService.cleanUp(Instant.now());
      metadataRepository.findAll().forEach(table -> {
        assertThat(table.getCleanupAttempts()).isEqualTo(finalI + 1);
        assertThat(table.getHousekeepingStatus()).isEqualTo(FAILED);
      });
    }
  }

  @Test
  @Timeout(value = 10)
  void doNotInfiniteLoopOnDryRunCleanup() {
    ExpiredMetadataHandler handler = new ExpiredMetadataHandler(metadataRepository, metadataCleaner, pathCleaner);
    pagingCleanupService = new PagingMetadataCleanupService(List.of(handler), 2, true);

    List<HousekeepingMetadata> tables = List
        .of(createHousekeepingMetadata("table1", SCHEDULED), createHousekeepingMetadata("table2", SCHEDULED),
            createHousekeepingMetadata("table3", SCHEDULED));
    metadataRepository.saveAll(tables);
    // TODO
    // List<HousekeepingPath> paths = List
    // .of(createEntityHousekeepingPath("s3://some_foo", SCHEDULED),
    // createEntityHousekeepingPath("s3://some_bar", SCHEDULED),
    // createEntityHousekeepingPath("s3://some_foobar", SCHEDULED));
    // housekeepingPathRepository.saveAll(paths);

    pagingCleanupService.cleanUp(Instant.now());

    metadataRepository.findAll().forEach(table -> {
      assertThat(table.getCleanupAttempts()).isEqualTo(0);
      assertThat(table.getHousekeepingStatus()).isEqualTo(SCHEDULED);
    });
  }

  // TODO
  // want this to be metadata or entity ?
  private HousekeepingMetadata createHousekeepingMetadata(String tableName, HousekeepingStatus housekeepingStatus) {
    HousekeepingMetadata metadata = new HousekeepingMetadata.Builder()
        // TODO
        // .path(path)
        .databaseName("database")
        // .tableName("table")
        .tableName(tableName)
        .housekeepingStatus(housekeepingStatus)
        .creationTimestamp(localNow)
        .modifiedTimestamp(localNow)
        .cleanupDelay(Duration.parse("P30D"))
        .cleanupAttempts(0)
        .lifecycleType(EXPIRED.toString())
        .build();

    metadata.setCleanupTimestamp(localNow);
    return metadata;
  }

}
