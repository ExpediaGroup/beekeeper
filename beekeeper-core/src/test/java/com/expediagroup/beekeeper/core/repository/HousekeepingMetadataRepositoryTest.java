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
package com.expediagroup.beekeeper.core.repository;

import static org.assertj.core.api.Assertions.assertThat;
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

  @Autowired
  private HousekeepingMetadataRepository housekeepingMetadataRepository;

  @BeforeEach
  public void setupDb() {
    housekeepingMetadataRepository.deleteAll();
  }

  @Test
  public void typicalSave() {
    HousekeepingMetadata table = createEntityHousekeepingTable();

    housekeepingMetadataRepository.save(table);

    List<HousekeepingMetadata> tables = housekeepingMetadataRepository.findAll();
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
    HousekeepingMetadata table = createEntityHousekeepingTable();
    HousekeepingMetadata savedTable = housekeepingMetadataRepository.save(table);

    savedTable.setHousekeepingStatus(DELETED);
    savedTable.setCleanupAttempts(savedTable.getCleanupAttempts() + 1);
    housekeepingMetadataRepository.save(savedTable);

    List<HousekeepingMetadata> tables = housekeepingMetadataRepository.findAll();
    assertThat(tables.size()).isEqualTo(1);
    HousekeepingMetadata updatedTable = tables.get(0);
    assertThat(updatedTable.getHousekeepingStatus()).isEqualTo(DELETED);
    assertThat(updatedTable.getCleanupAttempts()).isEqualTo(1);
    assertThat(updatedTable.getModifiedTimestamp()).isNotEqualTo(savedTable.getModifiedTimestamp());
  }

  @Test
  public void notNullableLifecycleTypeField() {
    HousekeepingMetadata table = createEntityHousekeepingTable();
    table.setLifecycleType(null);
    assertThrows(DataIntegrityViolationException.class, () -> housekeepingMetadataRepository.save(table));
  }

  @Test
  public void notNullableDatabaseNameField() {
    HousekeepingMetadata table = createEntityHousekeepingTable();
    table.setDatabaseName(null);
    assertThrows(DataIntegrityViolationException.class, () -> housekeepingMetadataRepository.save(table));
  }

  @Test
  public void notNullableTableNameField() {
    HousekeepingMetadata table = createEntityHousekeepingTable();
    table.setTableName(null);
    assertThrows(DataIntegrityViolationException.class, () -> housekeepingMetadataRepository.save(table));
  }

  @Test
  public void timezone() {
    HousekeepingMetadata path = createEntityHousekeepingTable();
    housekeepingMetadataRepository.save(path);
    List<HousekeepingMetadata> tables = housekeepingMetadataRepository.findAll();
    assertThat(tables.size()).isEqualTo(1);

    HousekeepingMetadata savedTable = tables.get(0);
    int utcHour = LocalDateTime.now(ZoneId.of("UTC")).getHour();
    assertThat(savedTable.getCleanupTimestamp().getHour()).isEqualTo(utcHour);
    assertThat(savedTable.getModifiedTimestamp().getHour()).isEqualTo(utcHour);
    assertThat(savedTable.getCreationTimestamp().getHour()).isEqualTo(utcHour);
  }

  @Test
  public void findRecordsForCleanupByModifiedTimestamp() {
    HousekeepingMetadata table = createEntityHousekeepingTable();
    housekeepingMetadataRepository.save(table);

    Page<HousekeepingMetadata> result = housekeepingMetadataRepository
        .findRecordsForCleanupByModifiedTimestamp(CLEANUP_TIMESTAMP, PageRequest.of(0, 500));
    assertThat(result.getContent().get(0).getDatabaseName()).isEqualTo(DATABASE_NAME);
    assertThat(result.getContent().get(0).getTableName()).isEqualTo(TABLE_NAME);
  }

  @Test
  public void findRecordsForCleanupByModifiedTimestampZeroResults() {
    HousekeepingMetadata table = createEntityHousekeepingTable();
    table.setHousekeepingStatus(DELETED);
    housekeepingMetadataRepository.save(table);

    Page<HousekeepingMetadata> result = housekeepingMetadataRepository
        .findRecordsForCleanupByModifiedTimestamp(LocalDateTime.now(), PageRequest.of(0, 500));
    assertThat(result.getContent().size()).isEqualTo(0);
  }

  @Test
  public void findRecordsForCleanupByModifiedTimestampMixedHousekeepingStatus() {
    HousekeepingMetadata housekeepingTable1 = createEntityHousekeepingTable();
    housekeepingMetadataRepository.save(housekeepingTable1);

    HousekeepingMetadata housekeepingTable2 = createEntityHousekeepingTable();
    housekeepingTable2.setHousekeepingStatus(FAILED);
    housekeepingMetadataRepository.save(housekeepingTable2);

    HousekeepingMetadata housekeepingTable3 = createEntityHousekeepingTable();
    housekeepingTable3.setHousekeepingStatus(DELETED);
    housekeepingMetadataRepository.save(housekeepingTable3);

    Page<HousekeepingMetadata> result = housekeepingMetadataRepository
        .findRecordsForCleanupByModifiedTimestamp(CLEANUP_TIMESTAMP, PageRequest.of(0, 500));
    assertThat(result.getContent().size()).isEqualTo(2);
  }

  @Test
  public void findRecordsForCleanupByModifiedTimestampRespectsOrder() {
    String table1 = "table1";
    String table2 = "table2";

    HousekeepingMetadata housekeepingTable1 = createEntityHousekeepingTable();
    housekeepingTable1.setTableName(table1);
    housekeepingMetadataRepository.save(housekeepingTable1);

    HousekeepingMetadata housekeepingTable2 = createEntityHousekeepingTable();
    housekeepingTable2.setTableName(table2);
    housekeepingMetadataRepository.save(housekeepingTable2);

    List<HousekeepingMetadata> result = housekeepingMetadataRepository
        .findRecordsForCleanupByModifiedTimestamp(CLEANUP_TIMESTAMP, PageRequest.of(0, 500))
        .getContent();
    assertThat(result.get(0).getDatabaseName()).isEqualTo(DATABASE_NAME);
    assertThat(result.get(0).getTableName()).isEqualTo(table1);
    assertThat(result.get(1).getDatabaseName()).isEqualTo(DATABASE_NAME);
    assertThat(result.get(1).getTableName()).isEqualTo(table2);
  }

  @Test
  public void findRecordForCleanupByDatabaseAndTable() {
    HousekeepingMetadata table = createEntityHousekeepingTable();
    housekeepingMetadataRepository.save(table);

    Optional<HousekeepingMetadata> result = housekeepingMetadataRepository.findRecordForCleanupByDatabaseAndTable(
        DATABASE_NAME, TABLE_NAME, PARTITION_NAME);

    assertTrue(result.isPresent());
    compare(result.get(), table);
  }

  @Test
  public void findRecordForCleanupByDatabaseAndTableZeroResults() {
    HousekeepingMetadata table = createEntityHousekeepingTable();
    table.setHousekeepingStatus(DELETED);
    housekeepingMetadataRepository.save(table);

    Optional<HousekeepingMetadata> result = housekeepingMetadataRepository.findRecordForCleanupByDatabaseAndTable(
        DATABASE_NAME, TABLE_NAME, PARTITION_NAME);

    assertTrue(result.isEmpty());
  }

  @Test
  public void findRecordForCleanupByDatabaseAndTableMixedHousekeepingStatus() {
    HousekeepingMetadata housekeepingTable1 = createEntityHousekeepingTable();
    housekeepingMetadataRepository.save(housekeepingTable1);

    HousekeepingMetadata housekeepingTable2 = createEntityHousekeepingTable();
    housekeepingTable2.setHousekeepingStatus(DELETED);
    housekeepingMetadataRepository.save(housekeepingTable2);

    Optional<HousekeepingMetadata> result = housekeepingMetadataRepository
        .findRecordForCleanupByDatabaseAndTable(DATABASE_NAME, TABLE_NAME, PARTITION_NAME);

    assertTrue(result.isPresent());
    compare(result.get(), housekeepingTable1);
  }

  // TODO
  // test for find records by database name and tbl name
  // test with partitioned table
  @Test
  public void findRecordsForGivenDatabaseAndTable() {
    HousekeepingMetadata housekeepingTable1 = createEntityHousekeepingTable();
    housekeepingMetadataRepository.save(housekeepingTable1);

    Page<HousekeepingMetadata> result = housekeepingMetadataRepository
        .findRecordsForGivenDatabaseAndTable(DATABASE_NAME, TABLE_NAME, PageRequest.of(0, 500));

    assertTrue(result.getContent().size() == 1);
    assertThat(result.getContent().get(0).getDatabaseName()).isEqualTo(DATABASE_NAME);
    assertThat(result.getContent().get(0).getTableName()).isEqualTo(TABLE_NAME);
  }

  private HousekeepingMetadata createEntityHousekeepingTable() {
    return new HousekeepingMetadata.Builder()
        .path(PATH)
        .databaseName(DATABASE_NAME)
        .tableName(TABLE_NAME)
        .partitionName(PARTITION_NAME)
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
