/**
 * Copyright (C) 2019-2023 Expedia, Inc.
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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DELETED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DISABLED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.FAILED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SKIPPED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.testcontainers.shaded.com.google.common.collect.Lists;

import com.expediagroup.beekeeper.cleanup.hive.HiveClient;
import com.expediagroup.beekeeper.cleanup.hive.HiveClientFactory;
import com.expediagroup.beekeeper.cleanup.metadata.MetadataCleaner;
import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.core.model.PeriodDuration;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;
import com.expediagroup.beekeeper.core.service.BeekeeperHistoryService;
import com.expediagroup.beekeeper.metadata.cleanup.TestApplication;
import com.expediagroup.beekeeper.metadata.cleanup.handler.ExpiredMetadataHandler;
import com.expediagroup.beekeeper.metadata.cleanup.handler.MetadataHandler;

@ExtendWith(SpringExtension.class)
@ExtendWith(MockitoExtension.class)
@ContextConfiguration(classes = { TestApplication.class }, loader = AnnotationConfigContextLoader.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class PagingMetadataCleanupServiceTest {

  public static final List<String> TABLE_PATHS = List.of("s3://bucket/table", "s3://bucket/table", "s3://bucket/table");
  public static final List<String> PARTITION_PATHS = List
      .of("s3://bucket/table/1", "s3://bucket/table/2", "s3://bucket/table/3");
  private final LocalDateTime localNow = LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
  private PagingMetadataCleanupService pagingCleanupService;
  private @Captor ArgumentCaptor<HousekeepingMetadata> metadataCaptor;
  private @Captor ArgumentCaptor<HousekeepingPath> pathCaptor;
  private @Captor ArgumentCaptor<HiveClient> hiveClientCaptor;
  private @Autowired HousekeepingMetadataRepository metadataRepository;
  private @MockBean MetadataCleaner metadataCleaner;
  private @MockBean PathCleaner pathCleaner;
  private @MockBean HiveClientFactory hiveClientFactory;
  private @MockBean HiveClient hiveClient;
  private @Mock BeekeeperHistoryService beekeeperHistoryService;

  private static final String PARTITION_NAME = "event_date=2020-01-01/event_hour=0/event_type=A";

  private ExpiredMetadataHandler handler;
  private List<MetadataHandler> handlers = new ArrayList<>();

  @BeforeEach
  public void init() {
    when(metadataCleaner.tableExists(Mockito.any(), Mockito.anyString(), Mockito.anyString())).thenReturn(true);
    when(metadataCleaner.dropPartition(Mockito.any(), Mockito.any())).thenReturn(true);
    Map<String, String> properties = new HashMap<>();
    properties.put(UNREFERENCED.getTableParameterName(), "true");
    properties.put("beekeeper.expired.data.table.deletion.enabled", "true");
    when(hiveClient.getTableProperties(Mockito.anyString(), Mockito.anyString()))
        .thenReturn(properties);
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    handler = new ExpiredMetadataHandler(hiveClientFactory, metadataRepository, metadataCleaner, pathCleaner,
        beekeeperHistoryService);
    handlers = List.of(handler);
    pagingCleanupService = new PagingMetadataCleanupService(handlers, 2, false);
  }

  @Test
  public void typicalUnpartitioned() {
    List<String> tables = List.of("table1", "table2", "table3");

    IntStream
        .range(0, tables.size())
        .forEach(i -> metadataRepository
            .save(createHousekeepingMetadata(tables.get(i), TABLE_PATHS.get(i), null, SCHEDULED)));

    pagingCleanupService.cleanUp(Instant.now());

    verify(metadataCleaner, times(3)).dropTable(metadataCaptor.capture(), hiveClientCaptor.capture());
    assertThat(metadataCaptor.getAllValues())
        .extracting("tableName")
        .containsExactly(tables.get(0), tables.get(1), tables.get(2));
    verify(pathCleaner, times(3)).cleanupPath(pathCaptor.capture());
    assertThat(pathCaptor.getAllValues())
        .extracting("path")
        .containsExactly(TABLE_PATHS.get(0), TABLE_PATHS.get(1), TABLE_PATHS.get(2));

    metadataRepository.findAll().forEach(housekeepingMetadata -> {
      assertThat(housekeepingMetadata.getCleanupAttempts()).isEqualTo(1);
      assertThat(housekeepingMetadata.getHousekeepingStatus()).isEqualTo(DELETED);
    });

    pagingCleanupService.cleanUp(Instant.now());
    verifyNoMoreInteractions(pathCleaner);
  }

  @Test
  public void typicalDryRunEnabled() {
    pagingCleanupService = new PagingMetadataCleanupService(handlers, 2, true);

    List<String> tables = List.of("table1", "table2", "table3");

    IntStream
        .range(0, tables.size())
        .forEach(i -> metadataRepository
            .save(createHousekeepingMetadata(tables.get(i), TABLE_PATHS.get(i), null, SCHEDULED)));

    pagingCleanupService.cleanUp(Instant.now());

    verify(metadataCleaner, times(3)).dropTable(metadataCaptor.capture(), hiveClientCaptor.capture());
    assertThat(metadataCaptor.getAllValues())
        .extracting("tableName")
        .containsExactly(tables.get(0), tables.get(1), tables.get(2));
    verify(pathCleaner, times(3)).cleanupPath(pathCaptor.capture());
    assertThat(pathCaptor.getAllValues())
        .extracting("path")
        .containsExactly(TABLE_PATHS.get(0), TABLE_PATHS.get(1), TABLE_PATHS.get(2));

    metadataRepository.findAll().forEach(housekeepingMetadata -> {
      assertThat(housekeepingMetadata.getCleanupAttempts()).isEqualTo(0);
      assertThat(housekeepingMetadata.getHousekeepingStatus()).isEqualTo(SCHEDULED);
    });
  }

  @Test
  public void typicalPartitioned() {
    List<String> tables = List.of("table1", "table2", "table3");

    IntStream
        .range(0, tables.size())
        .forEach(i -> metadataRepository
            .save(createHousekeepingMetadata(tables.get(i), PARTITION_PATHS.get(i), PARTITION_NAME, SCHEDULED)));

    pagingCleanupService.cleanUp(Instant.now());

    verify(metadataCleaner, times(3)).dropPartition(metadataCaptor.capture(), hiveClientCaptor.capture());
    assertThat(metadataCaptor.getAllValues())
        .extracting("tableName")
        .containsExactly(tables.get(0), tables.get(1), tables.get(2));
    verify(pathCleaner, times(3)).cleanupPath(pathCaptor.capture());
    assertThat(pathCaptor.getAllValues())
        .extracting("path")
        .containsExactly(PARTITION_PATHS.get(0), PARTITION_PATHS.get(1), PARTITION_PATHS.get(2));

    metadataRepository.findAll().forEach(housekeepingMetadata -> {
      assertThat(housekeepingMetadata.getCleanupAttempts()).isEqualTo(1);
      assertThat(housekeepingMetadata.getHousekeepingStatus()).isEqualTo(DELETED);
    });

    pagingCleanupService.cleanUp(Instant.now());
    verifyNoMoreInteractions(pathCleaner);
  }

  @Test
  public void mixOfScheduledAndFailedPaths() {
    List<HousekeepingMetadata> tables = List
        .of(createHousekeepingMetadata("table1", "s3://bucket/some_foo", null, SCHEDULED),
            createHousekeepingMetadata("table2", "s3://bucket/some_bar", null, FAILED));
    tables.forEach(table -> metadataRepository.save(table));

    pagingCleanupService.cleanUp(Instant.now());
    verify(metadataCleaner, times(2)).dropTable(metadataCaptor.capture(), hiveClientCaptor.capture());
    assertThat(metadataCaptor.getAllValues())
        .extracting("tableName")
        .containsExactly(tables.get(0).getTableName(), tables.get(1).getTableName());
    verify(pathCleaner, times(2)).cleanupPath(pathCaptor.capture());
    assertThat(pathCaptor.getAllValues())
        .extracting("path")
        .containsExactly(tables.get(0).getPath(), tables.get(1).getPath());
  }

  @Test
  public void mixOfAllPaths() {
    List<HousekeepingMetadata> tables = List
        .of(createHousekeepingMetadata("table1", "s3://bucket/some_foo", null, SCHEDULED),
            createHousekeepingMetadata("table2", "s3://bucket/some_bar", null, FAILED),
            createHousekeepingMetadata("table3", "s3://bucket/some_foobar", null, DELETED),
            createHousekeepingMetadata("table4", "s3://bucket/some_foobar", null, DISABLED),
            createHousekeepingMetadata("table5", "s3://bucket/some_foobar", null, SKIPPED));

    tables.forEach(path -> metadataRepository.save(path));
    pagingCleanupService.cleanUp(Instant.now());

    verify(metadataCleaner, times(2)).dropTable(metadataCaptor.capture(), hiveClientCaptor.capture());
    assertThat(metadataCaptor.getAllValues())
        .extracting("tableName")
        .containsExactly(tables.get(0).getTableName(), tables.get(1).getTableName());
    verify(pathCleaner, times(2)).cleanupPath(pathCaptor.capture());
    assertThat(pathCaptor.getAllValues())
        .extracting("path")
        .containsExactly(tables.get(0).getPath(), tables.get(1).getPath());
  }

  @Test
  public void metadataCleanerException() {
    Mockito
        .doNothing()
        .doThrow(new RuntimeException("Error"))
        .when(metadataCleaner)
        .dropTable(Mockito.any(HousekeepingMetadata.class), Mockito.any(HiveClient.class));

    List<HousekeepingMetadata> tables = List
        .of(createHousekeepingMetadata("table1", "s3://bucket/some_foo", null, SCHEDULED),
            createHousekeepingMetadata("table2", "s3://bucket/some_bar", null, SCHEDULED));
    tables.forEach(table -> metadataRepository.save(table));

    pagingCleanupService.cleanUp(Instant.now());

    verify(metadataCleaner, times(2)).dropTable(metadataCaptor.capture(), hiveClientCaptor.capture());
    assertThat(metadataCaptor.getAllValues())
        .extracting("tableName")
        .containsExactly(tables.get(0).getTableName(), tables.get(1).getTableName());

    List<HousekeepingMetadata> result = Lists.newArrayList(metadataRepository.findAll());
    assertThat(result.size()).isEqualTo(2);
    HousekeepingMetadata housekeepingMetadata1 = result.get(0);
    HousekeepingMetadata housekeepingMetadata2 = result.get(1);

    assertThat(housekeepingMetadata1.getTableName()).isEqualTo(tables.get(0).getTableName());
    assertThat(housekeepingMetadata1.getPath()).isEqualTo(tables.get(0).getPath());
    assertThat(housekeepingMetadata1.getHousekeepingStatus()).isEqualTo(DELETED);
    assertThat(housekeepingMetadata1.getCleanupAttempts()).isEqualTo(1);
    assertThat(housekeepingMetadata2.getTableName()).isEqualTo(tables.get(1).getTableName());
    assertThat(housekeepingMetadata2.getPath()).isEqualTo(tables.get(1).getPath());
    assertThat(housekeepingMetadata2.getHousekeepingStatus()).isEqualTo(FAILED);
    assertThat(housekeepingMetadata2.getCleanupAttempts()).isEqualTo(1);
  }

  @Test
  public void invalidPaths() {
    List<HousekeepingMetadata> tables = List
        .of(createHousekeepingMetadata("table1", "s3://invalid", null, SCHEDULED),
            createHousekeepingMetadata("table2", "s3://invalid/path", "partition", SCHEDULED));

    metadataRepository.saveAll(tables);
    pagingCleanupService.cleanUp(Instant.now());
    metadataRepository.findAll().forEach(table -> {
      assertThat(table.getCleanupAttempts()).isEqualTo(0);
      assertThat(table.getHousekeepingStatus()).isEqualTo(SKIPPED);
    });
  }

  @Test
  @Timeout(value = 10)
  void doNotInfiniteLoopOnRepeatedFailures() {
    List<HousekeepingMetadata> tables = List
        .of(createHousekeepingMetadata("table1", "s3://bucket/some_foo", null, FAILED),
            createHousekeepingMetadata("table2", "s3://bucket/some_bar", null, FAILED),
            createHousekeepingMetadata("table3", "s3://bucket/some_foobar", null, FAILED));

    doThrow(new RuntimeException("Error")).when(metadataCleaner).dropTable(Mockito.any(), Mockito.any());
    for (int i = 0; i < 5; i++) {
      int finalI = i;
      tables.forEach(path -> {
        if (finalI == 0) {
          metadataRepository.save(path);
        }
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
    pagingCleanupService = new PagingMetadataCleanupService(handlers, 2, true);

    List<HousekeepingMetadata> tables = List
        .of(createHousekeepingMetadata("table1", "s3://some_foo", null, SCHEDULED),
            createHousekeepingMetadata("table2", "s3://some_foo", null, SCHEDULED),
            createHousekeepingMetadata("table3", "s3://some_foo", null, SCHEDULED));
    metadataRepository.saveAll(tables);
    pagingCleanupService.cleanUp(Instant.now());
    metadataRepository.findAll().forEach(table -> {
      assertThat(table.getCleanupAttempts()).isEqualTo(0);
      assertThat(table.getHousekeepingStatus()).isEqualTo(SCHEDULED);
    });
  }

  private HousekeepingMetadata createHousekeepingMetadata(
      String tableName,
      String path,
      String partitionName,
      HousekeepingStatus housekeepingStatus) {
    HousekeepingMetadata metadata = HousekeepingMetadata
        .builder()
        .path(path)
        .databaseName("database")
        .tableName(tableName)
        .partitionName(partitionName)
        .housekeepingStatus(housekeepingStatus)
        .creationTimestamp(localNow)
        .modifiedTimestamp(localNow)
        .cleanupDelay(PeriodDuration.of(Duration.parse("P30D")))
        .cleanupAttempts(0)
        .lifecycleType(EXPIRED.toString())
        .build();

    metadata.setCleanupTimestamp(localNow);
    return metadata;
  }
}
