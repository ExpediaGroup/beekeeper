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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DELETED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.FAILED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

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
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;

@ExtendWith(SpringExtension.class)
@TestPropertySource(properties = {
    "hibernate.data-source.driver-class-name=org.h2.Driver",
    "hibernate.dialect=org.hibernate.dialect.H2Dialect",
    "hibernate.hbm2ddl.auto=create",
    "spring.jpa.show-sql=true",
    "spring.datasource.url=jdbc:h2:mem:beekeeper;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE;MODE=MySQL" })
@ContextConfiguration(classes = { TestApplication.class }, loader = AnnotationConfigContextLoader.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class HousekeepingMetadataRepositoryTest {

  private static final String PATH = "path";
  private static final String DATABASE_NAME = "database";
  private static final String TABLE_NAME = "table";
  private static final String PARTITION_NAME = "event_date=2020-01-01/event_hour=0/event_type=A";
  private static final LocalDateTime CREATION_TIMESTAMP = LocalDateTime.now(ZoneId.of("UTC"));
  private static final Duration CLEANUP_DELAY = Duration.parse("P3D");
  private static final LocalDateTime CLEANUP_TIMESTAMP = CREATION_TIMESTAMP.plus(CLEANUP_DELAY);

  private static final int PAGE = 0;
  private static final int PAGE_SIZE = 500;

  @Autowired
  private HousekeepingMetadataRepository housekeepingMetadataRepository;

  @BeforeEach
  public void setupDb() {
    housekeepingMetadataRepository.deleteAll();
  }

  @Test
  public void typicalSave() {
    HousekeepingMetadata table = createPartitionedEntityHousekeepingTable();

    housekeepingMetadataRepository.save(table);

    List<HousekeepingMetadata> tables = Lists.newArrayList(housekeepingMetadataRepository.findAll());
    assertThat(tables.size()).isEqualTo(1);
    HousekeepingMetadata savedTable = tables.get(0);
    assertThat(savedTable.getPath()).isEqualTo(PATH);
    assertThat(savedTable.getDatabaseName()).isEqualTo(DATABASE_NAME);
    assertThat(savedTable.getTableName()).isEqualTo(TABLE_NAME);
    assertThat(savedTable.getPartitionName()).isEqualTo(PARTITION_NAME);
    assertThat(savedTable.getHousekeepingStatus()).isEqualTo(SCHEDULED);
    assertThat(savedTable.getCleanupDelay()).isEqualTo(Duration.parse("P3D"));
    assertThat(savedTable.getCreationTimestamp()).isNotNull();
    assertThat(savedTable.getModifiedTimestamp()).isNotNull();
    assertThat(savedTable.getModifiedTimestamp()).isNotEqualTo(savedTable.getCreationTimestamp());
    assertThat(savedTable.getCleanupTimestamp()).isEqualTo(table.getCreationTimestamp().plus(table.getCleanupDelay()));
    assertThat(savedTable.getCleanupAttempts()).isEqualTo(0);
  }

  @Test
  public void typicalUpdate() {
    HousekeepingMetadata table = createPartitionedEntityHousekeepingTable();
    HousekeepingMetadata savedTable = housekeepingMetadataRepository.save(table);

    savedTable.setHousekeepingStatus(DELETED);
    savedTable.setCleanupAttempts(savedTable.getCleanupAttempts() + 1);
    housekeepingMetadataRepository.save(savedTable);

    List<HousekeepingMetadata> tables = Lists.newArrayList(housekeepingMetadataRepository.findAll());
    assertThat(tables.size()).isEqualTo(1);
    HousekeepingMetadata updatedTable = tables.get(0);
    assertThat(updatedTable.getHousekeepingStatus()).isEqualTo(DELETED);
    assertThat(updatedTable.getCleanupAttempts()).isEqualTo(1);
    assertThat(updatedTable.getModifiedTimestamp()).isNotEqualTo(savedTable.getModifiedTimestamp());
  }

  @Test
  public void notNullableLifecycleTypeField() {
    HousekeepingMetadata table = createPartitionedEntityHousekeepingTable();
    table.setLifecycleType(null);
    assertThrows(DataIntegrityViolationException.class, () -> housekeepingMetadataRepository.save(table));
  }

  @Test
  public void notNullableDatabaseNameField() {
    HousekeepingMetadata table = createPartitionedEntityHousekeepingTable();
    table.setDatabaseName(null);
    assertThrows(DataIntegrityViolationException.class, () -> housekeepingMetadataRepository.save(table));
  }

  @Test
  public void notNullableTableNameField() {
    HousekeepingMetadata table = createPartitionedEntityHousekeepingTable();
    table.setTableName(null);
    assertThrows(DataIntegrityViolationException.class, () -> housekeepingMetadataRepository.save(table));
  }

  @Test
  public void timezone() {
    HousekeepingMetadata path = createPartitionedEntityHousekeepingTable();
    housekeepingMetadataRepository.save(path);
    List<HousekeepingMetadata> tables = Lists.newArrayList(housekeepingMetadataRepository.findAll());
    assertThat(tables.size()).isEqualTo(1);

    HousekeepingMetadata savedTable = tables.get(0);
    int utcHour = LocalDateTime.now(ZoneId.of("UTC")).getHour();
    assertThat(savedTable.getCleanupTimestamp().getHour()).isEqualTo(utcHour);
    assertThat(savedTable.getModifiedTimestamp().getHour()).isEqualTo(utcHour);
    assertThat(savedTable.getCreationTimestamp().getHour()).isEqualTo(utcHour);
  }

  @Test
  public void findRecordsForCleanupByModifiedTimestamp() {
    HousekeepingMetadata table = createPartitionedEntityHousekeepingTable();
    housekeepingMetadataRepository.save(table);

    Page<HousekeepingMetadata> result = housekeepingMetadataRepository
        .findRecordsForCleanupByModifiedTimestamp(CLEANUP_TIMESTAMP, PageRequest.of(PAGE, PAGE_SIZE));
    assertThat(result.getContent().get(0).getDatabaseName()).isEqualTo(DATABASE_NAME);
    assertThat(result.getContent().get(0).getTableName()).isEqualTo(TABLE_NAME);
  }

  @Test
  public void findRecordsForCleanupByModifiedTimestampZeroResults() {
    HousekeepingMetadata table = createPartitionedEntityHousekeepingTable();
    table.setHousekeepingStatus(DELETED);
    housekeepingMetadataRepository.save(table);

    Page<HousekeepingMetadata> result = housekeepingMetadataRepository
        .findRecordsForCleanupByModifiedTimestamp(LocalDateTime.now(), PageRequest.of(PAGE, PAGE_SIZE));
    assertThat(result.getContent().size()).isEqualTo(0);
  }

  @Test
  public void findRecordsForCleanupByModifiedTimestampMixedHousekeepingStatus() {
    HousekeepingMetadata housekeepingTable1 = createPartitionedEntityHousekeepingTable();
    housekeepingMetadataRepository.save(housekeepingTable1);

    HousekeepingMetadata housekeepingTable2 = createPartitionedEntityHousekeepingTable();
    housekeepingTable2.setHousekeepingStatus(FAILED);
    housekeepingMetadataRepository.save(housekeepingTable2);

    HousekeepingMetadata housekeepingTable3 = createPartitionedEntityHousekeepingTable();
    housekeepingTable3.setHousekeepingStatus(DELETED);
    housekeepingMetadataRepository.save(housekeepingTable3);

    Page<HousekeepingMetadata> result = housekeepingMetadataRepository
        .findRecordsForCleanupByModifiedTimestamp(CLEANUP_TIMESTAMP, PageRequest.of(PAGE, PAGE_SIZE));
    assertThat(result.getContent().size()).isEqualTo(2);
  }

  @Test
  public void findRecordsForCleanupByModifiedTimestampRespectsOrder() {
    String table1 = "table1";
    String table2 = "table2";

    HousekeepingMetadata housekeepingTable1 = createPartitionedEntityHousekeepingTable();
    housekeepingTable1.setTableName(table1);
    housekeepingMetadataRepository.save(housekeepingTable1);

    HousekeepingMetadata housekeepingTable2 = createPartitionedEntityHousekeepingTable();
    housekeepingTable2.setTableName(table2);
    housekeepingMetadataRepository.save(housekeepingTable2);

    List<HousekeepingMetadata> result = housekeepingMetadataRepository
        .findRecordsForCleanupByModifiedTimestamp(CLEANUP_TIMESTAMP, PageRequest.of(PAGE, PAGE_SIZE))
        .getContent();
    assertThat(result.get(0).getDatabaseName()).isEqualTo(DATABASE_NAME);
    assertThat(result.get(0).getTableName()).isEqualTo(table1);
    assertThat(result.get(1).getDatabaseName()).isEqualTo(DATABASE_NAME);
    assertThat(result.get(1).getTableName()).isEqualTo(table2);
  }

  @Test
  public void findRecordForCleanupByDatabaseAndTable() {
    HousekeepingMetadata table = createPartitionedEntityHousekeepingTable();
    housekeepingMetadataRepository.save(table);

    Optional<HousekeepingMetadata> result = housekeepingMetadataRepository.findRecordForCleanupByDbTableAndPartitionName(
        DATABASE_NAME, TABLE_NAME, PARTITION_NAME);

    assertTrue(result.isPresent());
    compare(result.get(), table);
  }

  @Test
  public void findRecordForCleanupByDatabaseAndTableForNullCase() {
    HousekeepingMetadata table = createUnpartitionedEntityHousekeepingTable();
    table.setPartitionName(null);
    housekeepingMetadataRepository.save(table);

    Optional<HousekeepingMetadata> result = housekeepingMetadataRepository.findRecordForCleanupByDbTableAndPartitionName(
        DATABASE_NAME, TABLE_NAME, null);

    assertTrue(result.isPresent());
    compare(result.get(), table);
  }

  @Test
  public void findRecordForCleanupByDatabaseAndTableZeroResults() {
    HousekeepingMetadata table = createPartitionedEntityHousekeepingTable();
    table.setHousekeepingStatus(DELETED);
    housekeepingMetadataRepository.save(table);

    Optional<HousekeepingMetadata> result = housekeepingMetadataRepository.findRecordForCleanupByDbTableAndPartitionName(
        DATABASE_NAME, TABLE_NAME, PARTITION_NAME);

    assertTrue(result.isEmpty());
  }

  @Test
  public void findRecordForCleanupByDatabaseAndTableMixedHousekeepingStatus() {
    HousekeepingMetadata housekeepingTable1 = createPartitionedEntityHousekeepingTable();
    housekeepingMetadataRepository.save(housekeepingTable1);

    HousekeepingMetadata housekeepingTable2 = createPartitionedEntityHousekeepingTable();
    housekeepingTable2.setHousekeepingStatus(DELETED);
    housekeepingMetadataRepository.save(housekeepingTable2);

    Optional<HousekeepingMetadata> result = housekeepingMetadataRepository
        .findRecordForCleanupByDbTableAndPartitionName(DATABASE_NAME, TABLE_NAME, PARTITION_NAME);

    assertTrue(result.isPresent());
    compare(result.get(), housekeepingTable1);
  }

  @Test
  public void countPartitionsForGivenDatabaseAndTableWherePartitionIsNotNull() {
    HousekeepingMetadata housekeepingTable1 = createPartitionedEntityHousekeepingTable();
    housekeepingMetadataRepository.save(housekeepingTable1);

    HousekeepingMetadata housekeepingTable2 = createEntityHouseKeepingTable(DATABASE_NAME, TABLE_NAME + "2", null);
    housekeepingMetadataRepository.save(housekeepingTable2);

    long result = housekeepingMetadataRepository
        .countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE_NAME, TABLE_NAME);

    assertEquals(1L, result);
  }

  @Test
  public void countPartitionsForGivenDatabaseAndTableEmpty() {
    HousekeepingMetadata housekeepingTable1 = createEntityHouseKeepingTable(DATABASE_NAME + "1", TABLE_NAME + "1",
        PARTITION_NAME);
    housekeepingMetadataRepository.save(housekeepingTable1);

    long result = housekeepingMetadataRepository
        .countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE_NAME, TABLE_NAME);

    assertEquals(0L, result);
  }

  @Test
  public void countPartitionsForGivenDatabaseAndTableMultipleDatabaseEntries() {
    HousekeepingMetadata housekeepingTable1 = createPartitionedEntityHousekeepingTable();
    housekeepingMetadataRepository.save(housekeepingTable1);
    HousekeepingMetadata housekeepingTable2 = createEntityHouseKeepingTable(DATABASE_NAME + "1", TABLE_NAME + "1",
        PARTITION_NAME);
    housekeepingMetadataRepository.save(housekeepingTable2);

    long result = housekeepingMetadataRepository
        .countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE_NAME, TABLE_NAME);

    assertEquals(1L, result);
  }

  @Test
  public void countPartitionsForGivenDatabaseAndTableMixedHousekeepingStatus() {
    HousekeepingMetadata housekeepingTable1 = createPartitionedEntityHousekeepingTable();
    housekeepingTable1.setHousekeepingStatus(DELETED);
    housekeepingMetadataRepository.save(housekeepingTable1);
    HousekeepingMetadata housekeepingTable2 = createUnpartitionedEntityHousekeepingTable();
    housekeepingMetadataRepository.save(housekeepingTable2);

    long result = housekeepingMetadataRepository
        .countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE_NAME, TABLE_NAME);

    assertEquals(0L, result);
  }

  @Test
  public void dryRunCountPartitionsForPartitionedTable() {
    HousekeepingMetadata housekeepingTable = createPartitionedEntityHousekeepingTable();
    housekeepingMetadataRepository.save(housekeepingTable);

    long result = housekeepingMetadataRepository.countRecordsForDryRunWherePartitionIsNotNullOrExpired(
        CLEANUP_TIMESTAMP, DATABASE_NAME, TABLE_NAME);

    assertEquals(1L, result);
  }

  @Test
  public void dryRunCountPartitionsForPartitionedTableEmpty() {
    HousekeepingMetadata housekeepingTable = createPartitionedEntityHousekeepingTable();
    housekeepingTable.setHousekeepingStatus(DELETED);
    housekeepingMetadataRepository.save(housekeepingTable);

    long result = housekeepingMetadataRepository.countRecordsForDryRunWherePartitionIsNotNullOrExpired(
        CLEANUP_TIMESTAMP, DATABASE_NAME, TABLE_NAME);

    assertEquals(0L, result);
  }

  @Test
  public void dryRunCountPartitionsForUnpartitionedTable() {
    HousekeepingMetadata housekeepingTable = createUnpartitionedEntityHousekeepingTable();
    housekeepingMetadataRepository.save(housekeepingTable);

    long result = housekeepingMetadataRepository.countRecordsForDryRunWherePartitionIsNotNullOrExpired(
        CLEANUP_TIMESTAMP, DATABASE_NAME, TABLE_NAME);

    assertEquals(0L, result);
  }

  private HousekeepingMetadata createUnpartitionedEntityHousekeepingTable() {
    return createEntityHousekeepingTable(null);
  }

  private HousekeepingMetadata createPartitionedEntityHousekeepingTable() {
    return createEntityHousekeepingTable(PARTITION_NAME);
  }

  private HousekeepingMetadata createEntityHousekeepingTable(String partitionName) {
    return createEntityHouseKeepingTable(DATABASE_NAME, TABLE_NAME, partitionName);
  }

  private HousekeepingMetadata createEntityHouseKeepingTable(
      String databaseName,
      String tableName,
      String partitionName) {
    return HousekeepingMetadata.builder()
        .path(PATH)
        .databaseName(databaseName)
        .tableName(tableName)
        .partitionName(partitionName)
        .housekeepingStatus(SCHEDULED)
        .creationTimestamp(CREATION_TIMESTAMP)
        .modifiedTimestamp(CREATION_TIMESTAMP)
        .cleanupDelay(CLEANUP_DELAY)
        .cleanupAttempts(0)
        .lifecycleType(EXPIRED.toString())
        .build();
  }

  private void compare(HousekeepingMetadata expected, HousekeepingMetadata actual) {
    assertThat(actual.getPath()).isEqualTo(expected.getPath());
    assertThat(actual.getDatabaseName()).isEqualTo(expected.getDatabaseName());
    assertThat(actual.getTableName()).isEqualTo(expected.getTableName());
    assertThat(actual.getPartitionName()).isEqualTo(expected.getPartitionName());
    assertThat(actual.getHousekeepingStatus()).isEqualTo(expected.getHousekeepingStatus());
    assertThat(actual.getCreationTimestamp()).isEqualTo(expected.getCreationTimestamp());
    assertThat(actual.getModifiedTimestamp()).isEqualTo(expected.getModifiedTimestamp());
    assertThat(actual.getCleanupDelay()).isEqualTo(expected.getCleanupDelay());
    assertThat(actual.getCleanupAttempts()).isEqualTo(expected.getCleanupAttempts());
    assertThat(actual.getLifecycleType()).isEqualTo(expected.getLifecycleType());
  }
}
