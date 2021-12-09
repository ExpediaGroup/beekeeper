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
package com.expediagroup.beekeeper.metadata.cleanup.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DISABLED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.collect.Lists;

import com.expediagroup.beekeeper.cleanup.hive.HiveClient;
import com.expediagroup.beekeeper.cleanup.hive.HiveClientFactory;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;
import com.expediagroup.beekeeper.metadata.cleanup.TestApplication;

@ExtendWith(SpringExtension.class)
@ExtendWith(MockitoExtension.class)
@ContextConfiguration(classes = { TestApplication.class }, loader = AnnotationConfigContextLoader.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class MetadataDisableTablesServiceTest {

  private final LocalDateTime localNow = LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);

  private MetadataDisableTablesService disableTablesService;
  private @Autowired HousekeepingMetadataRepository metadataRepository;
  private @MockBean HiveClientFactory hiveClientFactory;
  private @MockBean HiveClient hiveClient;

  @BeforeEach
  public void init() {
    Map<String, String> properties = new HashMap<>();
    properties.put(UNREFERENCED.getTableParameterName(), "true");
    when(hiveClient.getTableProperties(Mockito.any(), Mockito.any())).thenReturn(properties);
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    disableTablesService = new MetadataDisableTablesService(hiveClientFactory, metadataRepository, false);
  }

  @Test
  @Transactional
  public void disabledUnpartitioned() {
    // table2 disabled, table1 and table3 enabled
    when(hiveClient.getTableProperties("database", "table2")).thenReturn(new HashMap<>());

    List<String> paths = List.of("s3://some_foo", "s3://some_bar", "s3://some_foobar");
    List<String> tables = List.of("table1", "table2", "table3");

    IntStream
        .range(0, tables.size())
        .forEach(
            i -> metadataRepository.save(createHousekeepingMetadata(tables.get(i), paths.get(i), null, SCHEDULED)));

    disableTablesService.disable();

    List<HousekeepingMetadata> records = Lists.newArrayList(metadataRepository.findAll());
    assertThat(records.size()).isEqualTo(3);
    assertThat(records.get(0).getCleanupAttempts()).isEqualTo(0);
    assertThat(records.get(0).getHousekeepingStatus()).isEqualTo(SCHEDULED);
    assertThat(records.get(1).getHousekeepingStatus()).isEqualTo(DISABLED);
    assertThat(records.get(2).getHousekeepingStatus()).isEqualTo(SCHEDULED);
  }

  @Test
  @Transactional
  public void disabledPartitioned() {
    // table1 and table2 disabled, table3 enabled
    when(hiveClient.getTableProperties("database", "table1")).thenReturn(new HashMap<>());
    when(hiveClient.getTableProperties("database", "table2")).thenReturn(new HashMap<>());

    List<String> paths = List.of("s3://some_foo", "s3://some_bar", "s3://some_foobar");
    List<String> tables = List.of("table1", "table2", "table3");

    // insert 3 table records + 3 corresponding partition records
    IntStream
        .range(0, tables.size())
        .forEach(i -> metadataRepository
            .save(createHousekeepingMetadata(tables.get(i), paths.get(i), null, SCHEDULED)));
    IntStream
        .range(0, tables.size())
        .forEach(i -> metadataRepository
            .save(createHousekeepingMetadata(tables.get(i), paths.get(i), "partition", SCHEDULED)));

    disableTablesService.disable();

    List<HousekeepingMetadata> records = Lists.newArrayList(metadataRepository.findAll());
    // there should be 3 table records and 1 partition record for the scheduled table
    assertThat(records.size()).isEqualTo(4);
    assertThat(records.get(0).getHousekeepingStatus()).isEqualTo(DISABLED);
    assertThat(records.get(0).getPartitionName()).isEqualTo(null);
    assertThat(records.get(1).getHousekeepingStatus()).isEqualTo(DISABLED);
    assertThat(records.get(1).getPartitionName()).isEqualTo(null);
    assertThat(records.get(2).getHousekeepingStatus()).isEqualTo(SCHEDULED);
    assertThat(records.get(2).getPartitionName()).isEqualTo(null);
    assertThat(records.get(3).getHousekeepingStatus()).isEqualTo(SCHEDULED);
    assertThat(records.get(3).getPartitionName()).isNotNull();
  }

  @Test
  @Transactional
  public void disabledDryRun() {
    disableTablesService = new MetadataDisableTablesService(hiveClientFactory, metadataRepository, true);
    // all tables disabled
    when(hiveClient.getTableProperties(any(), any())).thenReturn(new HashMap<>());

    List<String> paths = List.of("s3://some_foo", "s3://some_bar", "s3://some_foobar");
    List<String> tables = List.of("table1", "table2", "table3");

    IntStream
        .range(0, tables.size())
        .forEach(
            i -> metadataRepository.save(createHousekeepingMetadata(tables.get(i), paths.get(i), null, SCHEDULED)));

    disableTablesService.disable();

    List<HousekeepingMetadata> records = Lists.newArrayList(metadataRepository.findAll());
    assertThat(records.size()).isEqualTo(3);
    assertThat(records.get(0).getCleanupAttempts()).isEqualTo(0);
    assertThat(records.get(0).getHousekeepingStatus()).isEqualTo(SCHEDULED);
    assertThat(records.get(1).getHousekeepingStatus()).isEqualTo(SCHEDULED);
    assertThat(records.get(2).getHousekeepingStatus()).isEqualTo(SCHEDULED);
  }

  @Test
  @Transactional
  public void noTablesToDisableUnpartitioned() {
    List<String> paths = List.of("s3://some_foo", "s3://some_bar", "s3://some_foobar");
    List<String> tables = List.of("table1", "table2", "table3");

    IntStream
        .range(0, tables.size())
        .forEach(
            i -> metadataRepository.save(createHousekeepingMetadata(tables.get(i), paths.get(i), null, SCHEDULED)));

    disableTablesService.disable();

    List<HousekeepingMetadata> records = Lists.newArrayList(metadataRepository.findAll());
    assertThat(records.size()).isEqualTo(3);
    assertThat(records.get(0).getCleanupAttempts()).isEqualTo(0);
    assertThat(records.get(0).getHousekeepingStatus()).isEqualTo(SCHEDULED);
    assertThat(records.get(1).getHousekeepingStatus()).isEqualTo(SCHEDULED);
    assertThat(records.get(2).getHousekeepingStatus()).isEqualTo(SCHEDULED);
  }

  private HousekeepingMetadata createHousekeepingMetadata(
      String tableName,
      String path,
      String partitionName,
      HousekeepingStatus housekeepingStatus) {
    HousekeepingMetadata metadata = HousekeepingMetadata.builder()
        .path(path)
        .databaseName("database")
        .tableName(tableName)
        .partitionName(partitionName)
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
