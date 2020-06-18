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
import com.expediagroup.beekeeper.core.model.EntityHousekeepingTable;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;

@ExtendWith(SpringExtension.class)
@TestPropertySource(properties = {
    "hibernate.data-source.driver-class-name=org.h2.Driver",
    "hibernate.dialect=org.hibernate.dialect.H2Dialect",
    "hibernate.hbm2ddl.auto=create",
    "spring.jpa.show-sql=true",
    "spring.datasource.url=jdbc:h2:mem:beekeeper;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE;MODE=MySQL" })
@ContextConfiguration(classes = { TestApplication.class }, loader = AnnotationConfigContextLoader.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class HousekeepingTableRepositoryTest {

  private static String DATABASE_NAME = "database";
  private static String TABLE_NAME = "table";

  @Autowired
  private HousekeepingTableRepository housekeepingTableRepository;

  @BeforeEach
  public void setupDb() {
    housekeepingTableRepository.deleteAll();
  }

  @Test
  public void typicalSave() {
    EntityHousekeepingTable table = createEntityHousekeepingTable();

    housekeepingTableRepository.save(table);

    List<EntityHousekeepingTable> tables = housekeepingTableRepository.findAll();
    assertThat(tables.size()).isEqualTo(1);
    EntityHousekeepingTable savedTable = tables.get(0);
    assertThat(savedTable.getDatabaseName()).isEqualTo(DATABASE_NAME);
    assertThat(savedTable.getTableName()).isEqualTo(TABLE_NAME);
    assertThat(savedTable.getHousekeepingStatus()).isEqualTo(HousekeepingStatus.SCHEDULED);
    assertThat(savedTable.getCleanupDelay()).isEqualTo(Duration.parse("P3D"));
    assertThat(savedTable.getCreationTimestamp()).isNotNull();
    assertThat(savedTable.getModifiedTimestamp()).isNotNull();
    assertThat(savedTable.getModifiedTimestamp()).isNotEqualTo(savedTable.getCreationTimestamp());
    assertThat(savedTable.getCleanupTimestamp()).isEqualTo(table.getCreationTimestamp().plus(table.getCleanupDelay()));
    assertThat(savedTable.getCleanupAttempts()).isEqualTo(0);
  }

  @Test
  public void typicalUpdate() {
    EntityHousekeepingTable table = createEntityHousekeepingTable();
    EntityHousekeepingTable savedTable = housekeepingTableRepository.save(table);

    savedTable.setHousekeepingStatus(DELETED);
    savedTable.setCleanupAttempts(savedTable.getCleanupAttempts() + 1);
    housekeepingTableRepository.save(savedTable);

    List<EntityHousekeepingTable> tables = housekeepingTableRepository.findAll();
    assertThat(tables.size()).isEqualTo(1);
    EntityHousekeepingTable updatedTable = tables.get(0);
    assertThat(updatedTable.getHousekeepingStatus()).isEqualTo(DELETED);
    assertThat(updatedTable.getCleanupAttempts()).isEqualTo(1);
    assertThat(updatedTable.getModifiedTimestamp()).isNotEqualTo(savedTable.getModifiedTimestamp());
  }

  @Test
  public void notNullableLifecycleTypeField() {
    EntityHousekeepingTable table = createEntityHousekeepingTable();
    table.setLifecycleType(null);
    assertThrows(DataIntegrityViolationException.class, () -> housekeepingTableRepository.save(table));
  }

  @Test
  public void notNullableDatabaseNameField() {
    EntityHousekeepingTable table = createEntityHousekeepingTable();
    table.setDatabaseName(null);
    assertThrows(DataIntegrityViolationException.class, () -> housekeepingTableRepository.save(table));
  }

  @Test
  public void notNullableTableNameField() {
    EntityHousekeepingTable table = createEntityHousekeepingTable();
    table.setTableName(null);
    assertThrows(DataIntegrityViolationException.class, () -> housekeepingTableRepository.save(table));
  }

  @Test
  public void timezone() {
    EntityHousekeepingTable path = createEntityHousekeepingTable();
    housekeepingTableRepository.save(path);
    List<EntityHousekeepingTable> tables = housekeepingTableRepository.findAll();
    assertThat(tables.size()).isEqualTo(1);

    EntityHousekeepingTable savedTable = tables.get(0);
    int utcHour = LocalDateTime.now(ZoneId.of("UTC")).getHour();
    assertThat(savedTable.getCleanupTimestamp().getHour()).isEqualTo(utcHour);
    assertThat(savedTable.getModifiedTimestamp().getHour()).isEqualTo(utcHour);
    assertThat(savedTable.getCreationTimestamp().getHour()).isEqualTo(utcHour);
  }

  @Test
  public void findRecordsForCleanupByModifiedTimestamp() {
    EntityHousekeepingTable table = createEntityHousekeepingTable();
    table.setCleanupTimestamp(LocalDateTime.now(ZoneId.of("UTC")));
    housekeepingTableRepository.save(table);

    Page<EntityHousekeepingTable> result = housekeepingTableRepository
        .findRecordsForCleanupByModifiedTimestamp(LocalDateTime.now(ZoneId.of("UTC")), PageRequest.of(0, 500));
    assertThat(result.getContent().get(0).getDatabaseName()).isEqualTo(DATABASE_NAME);
    assertThat(result.getContent().get(0).getTableName()).isEqualTo(TABLE_NAME);
  }

  @Test
  public void findRecordsForCleanupByModifiedTimestampZeroResults() {
    EntityHousekeepingTable table = createEntityHousekeepingTable();
    table.setHousekeepingStatus(DELETED);
    housekeepingTableRepository.save(table);

    Page<EntityHousekeepingTable> result = housekeepingTableRepository
        .findRecordsForCleanupByModifiedTimestamp(LocalDateTime.now(), PageRequest.of(0, 500));
    assertThat(result.getContent().size()).isEqualTo(0);
  }

  @Test
  public void findRecordsForCleanupByModifiedTimestampMixedHousekeepingStatus() {
    LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));

    EntityHousekeepingTable housekeepingTable1 = createEntityHousekeepingTable();
    housekeepingTable1.setCleanupTimestamp(now);
    housekeepingTableRepository.save(housekeepingTable1);

    EntityHousekeepingTable housekeepingTable2 = createEntityHousekeepingTable();
    housekeepingTable2.setCleanupTimestamp(now);
    housekeepingTable2.setHousekeepingStatus(FAILED);
    housekeepingTableRepository.save(housekeepingTable2);

    EntityHousekeepingTable housekeepingTable3 = createEntityHousekeepingTable();
    housekeepingTable3.setCleanupTimestamp(now);
    housekeepingTable3.setHousekeepingStatus(DELETED);
    housekeepingTableRepository.save(housekeepingTable3);

    Page<EntityHousekeepingTable> result = housekeepingTableRepository
        .findRecordsForCleanupByModifiedTimestamp(LocalDateTime.now(ZoneId.of("UTC")), PageRequest.of(0, 500));
    assertThat(result.getContent().size()).isEqualTo(2);
  }

  @Test
  public void findRecordsForCleanupByModifiedTimestampRespectsOrder() {
    LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
    String table1 = "table1";
    String table2 = "table2";

    EntityHousekeepingTable housekeepingTable1 = createEntityHousekeepingTable();
    housekeepingTable1.setCleanupTimestamp(now);
    housekeepingTable1.setTableName(table1);
    housekeepingTableRepository.save(housekeepingTable1);

    EntityHousekeepingTable housekeepingTable2 = createEntityHousekeepingTable();
    housekeepingTable2.setCleanupTimestamp(now);
    housekeepingTable2.setTableName(table2);
    housekeepingTableRepository.save(housekeepingTable2);

    List<EntityHousekeepingTable> result = housekeepingTableRepository
        .findRecordsForCleanupByModifiedTimestamp(LocalDateTime.now(ZoneId.of("UTC")), PageRequest.of(0, 500))
        .getContent();
    assertThat(result.get(0).getDatabaseName()).isEqualTo(DATABASE_NAME);
    assertThat(result.get(0).getTableName()).isEqualTo(table1);
    assertThat(result.get(1).getDatabaseName()).isEqualTo(DATABASE_NAME);
    assertThat(result.get(1).getTableName()).isEqualTo(table2);
  }

  @Test
  public void findRecordForCleanupByDatabaseAndTable() {
    EntityHousekeepingTable table = createEntityHousekeepingTable();
    housekeepingTableRepository.save(table);

    Optional<EntityHousekeepingTable> result = housekeepingTableRepository.findRecordForCleanupByDatabaseAndTable(
        DATABASE_NAME, TABLE_NAME);

    assertTrue(result.isPresent());
    compare(result.get(), table);
  }

  @Test
  public void findRecordForCleanupByDatabaseAndTableZeroResults() {
    EntityHousekeepingTable table = createEntityHousekeepingTable();
    table.setHousekeepingStatus(DELETED);
    housekeepingTableRepository.save(table);

    Optional<EntityHousekeepingTable> result = housekeepingTableRepository.findRecordForCleanupByDatabaseAndTable(
        DATABASE_NAME, TABLE_NAME);

    assertTrue(result.isEmpty());
  }

  @Test
  public void findRecordForCleanupByDatabaseAndTableMixedHousekeepingStatus() {
    EntityHousekeepingTable housekeepingTable1 = createEntityHousekeepingTable();
    housekeepingTableRepository.save(housekeepingTable1);

    EntityHousekeepingTable housekeepingTable2 = createEntityHousekeepingTable();
    housekeepingTable2.setHousekeepingStatus(DELETED);
    housekeepingTableRepository.save(housekeepingTable2);

    Optional<EntityHousekeepingTable> result = housekeepingTableRepository
        .findRecordForCleanupByDatabaseAndTable(DATABASE_NAME, TABLE_NAME);

    assertTrue(result.isPresent());
    compare(result.get(), housekeepingTable1);
  }

  private EntityHousekeepingTable createEntityHousekeepingTable() {
    LocalDateTime creationTimestamp = LocalDateTime.now(ZoneId.of("UTC"));
    return new EntityHousekeepingTable.Builder()
        .databaseName(DATABASE_NAME)
        .tableName(TABLE_NAME)
        .housekeepingStatus(HousekeepingStatus.SCHEDULED)
        .creationTimestamp(creationTimestamp)
        .modifiedTimestamp(creationTimestamp)
        .cleanupDelay(Duration.parse("P3D"))
        .cleanupAttempts(0)
        .lifecycleType(EXPIRED.toString())
        .build();
  }

  private void compare(EntityHousekeepingTable actual, EntityHousekeepingTable expected) {
    assertThat(actual.getDatabaseName()).isEqualTo(expected.getDatabaseName());
    assertThat(actual.getTableName()).isEqualTo(expected.getTableName());
    assertThat(actual.getHousekeepingStatus()).isEqualTo(expected.getHousekeepingStatus());
    assertThat(actual.getCreationTimestamp()).isEqualTo(expected.getCreationTimestamp());
    assertThat(actual.getModifiedTimestamp()).isEqualTo(expected.getModifiedTimestamp());
    assertThat(actual.getCleanupDelay()).isEqualTo(expected.getCleanupDelay());
    assertThat(actual.getCleanupAttempts()).isEqualTo(expected.getCleanupAttempts());
    assertThat(actual.getLifecycleType()).isEqualTo(expected.getLifecycleType());
  }
}
