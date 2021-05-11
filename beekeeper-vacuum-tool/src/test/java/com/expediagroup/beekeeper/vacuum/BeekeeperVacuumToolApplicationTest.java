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
package com.expediagroup.beekeeper.vacuum;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import ch.qos.logback.classic.spi.ILoggingEvent;

import com.google.common.base.Supplier;

import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.scheduler.service.SchedulerService;
import com.expediagroup.beekeeper.vacuum.repository.BeekeeperRepository;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@ExtendWith(SpringExtension.class)
@ExtendWith(MockitoExtension.class)
@TestPropertySource(properties = {
    "metastore-uri=thrift://localhost:1234",
    "database=test_db",
    "table=test_table",
    "default-cleanup-delay=P1D",
    "dry-run=false",
    "spring.jpa.hibernate.ddl-auto=update",
    "spring.jpa.database=default" })
@ContextConfiguration(classes = { TestApplication.class })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class BeekeeperVacuumToolApplicationTest {

  private final String databaseName = "test_db";
  private final String tableName = "test_table";
  private final String expiryTime = "P1D";
  private final Table table = new Table();
  private final ApplicationArguments args = new DefaultApplicationArguments(new String[0]);
  private Path snapshot0Dir = null;
  private Path snapshot1Dir;
  private Path tableDir;
  private Path baseDir;
  private Path partition1InSnapshot0Dir;
  private Path partition2InSnapshot0Dir;
  private @Autowired BeekeeperRepository repository;
  private @Autowired HiveConf conf;
  private @MockBean Supplier<CloseableMetaStoreClient> clientSupplier;
  private @MockBean SchedulerService schedulerService;
  private @Mock CloseableMetaStoreClient closeableMetaStoreClient;
  private @Captor ArgumentCaptor<HousekeepingPath> housekeepingPath;
  private BeekeeperVacuumToolApplication application;
  private TestAppender appender = new TestAppender();

  @BeforeEach
  void setUp() throws TException, IOException {
    baseDir = Files.createTempDirectory("BeekeeperVacuumToolApplicationTest");
    tableDir = Files.createTempDirectory(baseDir, "table_");
    snapshot1Dir = Files.createTempDirectory(tableDir, "snapshot1_");

    table.setDbName(databaseName);
    table.setTableName(tableName);

    when(clientSupplier.get()).thenReturn(closeableMetaStoreClient);
    when(closeableMetaStoreClient.getTable(databaseName, tableName)).thenReturn(table);

    appender.clear();
  }

  @AfterEach
  void tearDown() throws IOException {
    FileUtils.deleteDirectory(baseDir.toFile());
  }

  @Test
  void typicalRunUnpartitioned() throws IOException {
    initialiseApp();
    setUnpartitionedTable();
    application.run(args);

    verify(schedulerService).scheduleForHousekeeping(housekeepingPath.capture());
    HousekeepingPath path = housekeepingPath.getValue();
    assertThat(path.getDatabaseName()).isEqualTo(databaseName);
    assertThat(path.getTableName()).isEqualTo(tableName);
    assertThat(path.getPath()).isEqualTo("file:" + snapshot0Dir.toString());
    assertThat(path.getHousekeepingStatus()).isEqualTo(SCHEDULED);
    assertThat(path.getCleanupDelay().toDays()).isEqualTo(1L);
    assertThat(path.getLifecycleType()).isEqualTo(UNREFERENCED.name());
  }

  @Test
  void typicalRunPartitioned() throws TException, IOException {
    initialiseApp();
    setPartitionedTable("partition1_");
    application.run(args);

    verify(schedulerService, times(2)).scheduleForHousekeeping(housekeepingPath.capture());
    List<HousekeepingPath> scheduledPaths = housekeepingPath.getAllValues();

    String file1Path = "file:" + partition1InSnapshot0Dir.toString();
    String file2Path = "file:" + partition2InSnapshot0Dir.toString();
    Set<String> paths = Set.of(scheduledPaths.get(0).getPath(), scheduledPaths.get(1).getPath());
    assertThat(paths).contains(file1Path);
    assertThat(paths).contains(file2Path);
    assertThat(assertBytesLogged(14)).isTrue();

    for (HousekeepingPath path : scheduledPaths) {
      assertThat(path.getDatabaseName()).isEqualTo(databaseName);
      assertThat(path.getTableName()).isEqualTo(tableName);
      assertThat(path.getHousekeepingStatus()).isEqualTo(SCHEDULED);
      assertThat(path.getCleanupDelay().toDays()).isEqualTo(1L);
      assertThat(path.getLifecycleType()).isEqualTo(UNREFERENCED.name());
    }
  }

  @Test
  void dryRun() throws IOException {
    initialiseDryRunApp();
    setUnpartitionedTable();
    application.run(args);
    verifyNoInteractions(schedulerService);
  }

  @Test
  void vacuumAfterPathWasScheduledForHousekeeping() throws IOException {
    initialiseApp();
    setUnpartitionedTable();
    HousekeepingPath path = HousekeepingPath.builder().databaseName(databaseName)
        .tableName(tableName)
        .path("file:" + snapshot0Dir.toString())
        .housekeepingStatus(SCHEDULED)
        .creationTimestamp(LocalDateTime.now())
        .cleanupDelay(Duration.parse("P3D"))
        .lifecycleType(UNREFERENCED.toString())
        .build();
    repository.save(path);

    application.run(args);
    verifyNoInteractions(schedulerService);
  }

  @Test
  void vacuumWithFilesThatShouldNotBeScheduled() throws TException, IOException {
    initialiseApp();
    setPartitionedTable("partition1_");

    // create hidden directories
    Files.createTempDirectory(snapshot1Dir, ".hidden1_");
    Files.createTempDirectory(snapshot1Dir, "_hidden2_");
    // create directories at levels that shouldn't be seen
    Files.createTempDirectory(tableDir, "snapshot3_");

    application.run(args);
    // paths scheduled are the same as the ones in 'typicalRunPartitioned'
    verify(schedulerService, times(2)).scheduleForHousekeeping(housekeepingPath.capture());
    assertThat(assertBytesLogged(14)).isTrue();
  }

  /**
   * Tests that spaces are handled correctly in three scenarios all set up in snapshot 1:
   * (1) 'partition 1_' in snapshot 1 is kept as it is referenced in the metastore
   * (2) 'partition 2_' in snapshot 1 is scheduled as it is not in the metastore
   * (3) 'partition 3_' in snapshot 1 is not scheduled as it is already in the housekeeping database
   */
  @Test
  void vacuumWithSpaceInPaths() throws IOException, TException {
    initialiseApp();
    setPartitionedTable("partition 1_");

    Path partition2InSnapshot1Dir = Files.createTempDirectory(snapshot1Dir, "partition 2_");
    File file2 = File.createTempFile("000000_", null, partition2InSnapshot1Dir.toFile());
    writeContentToFiles(file2.toPath(), file2.toPath(), file2.toPath());

    Path partition3InSnapshot1Dir = Files.createTempDirectory(snapshot1Dir, "partition 3_");
    File file3 = File.createTempFile("000000_", null, partition3InSnapshot1Dir.toFile());
    writeContentToFiles(file3.toPath(), file3.toPath(), file3.toPath());
    HousekeepingPath path = HousekeepingPath.builder().databaseName(databaseName)
        .tableName(tableName)
        .path("file:" + partition3InSnapshot1Dir.toString())
        .housekeepingStatus(SCHEDULED)
        .creationTimestamp(LocalDateTime.now())
        .cleanupDelay(Duration.parse("P3D"))
        .lifecycleType(UNREFERENCED.toString())
        .build();
    repository.save(path);

    application.run(args);

    verify(schedulerService, times(3)).scheduleForHousekeeping(housekeepingPath.capture());
    List<HousekeepingPath> scheduledPaths = housekeepingPath.getAllValues();

    String file1Path = "file:" + partition1InSnapshot0Dir.toString();
    String file2Path = "file:" + partition2InSnapshot0Dir.toString();
    String file3Path = "file:" + partition2InSnapshot1Dir.toString();
    assertThat(scheduledPaths).hasSize(3);
    assertThat(scheduledPaths).extracting("path")
        .containsExactlyInAnyOrder(file1Path, file2Path, file3Path);
    assertThat(assertBytesLogged(21)).isTrue();
  }

  private void initialiseApp() {
    application = new BeekeeperVacuumToolApplication(clientSupplier, schedulerService, repository, conf, databaseName,
        tableName, expiryTime, false, (short) 1000);
  }

  private void initialiseDryRunApp() {
    application = new BeekeeperVacuumToolApplication(clientSupplier, schedulerService, repository, conf, databaseName,
        tableName, expiryTime, true, (short) 1000);
  }

  private void setPartitionedTable(String metastorePathPrefix) throws TException, IOException {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("file:" + tableDir.toAbsolutePath().toString());
    table.setSd(sd);

    FieldSchema schema = new FieldSchema();
    table.setPartitionKeys(Collections.singletonList(schema));

    Path partitionInSnapshot1Dir = Files.createTempDirectory(snapshot1Dir, metastorePathPrefix);
    Partition partition = new Partition();
    StorageDescriptor partitionSd = new StorageDescriptor();
    partitionSd.setLocation("file:" + partitionInSnapshot1Dir.toAbsolutePath().toString());
    partition.setSd(partitionSd);
    List<String> partitionNameAsList = Collections.singletonList(metastorePathPrefix);
    List<Partition> partitionAsList = Collections.singletonList(partition);

    when(closeableMetaStoreClient.listPartitions(databaseName, tableName, (short) 1)).thenReturn(partitionAsList);
    when(closeableMetaStoreClient.listPartitionNames(databaseName, tableName, (short) -1)).thenReturn(
        partitionNameAsList);
    when(closeableMetaStoreClient.getPartitionsByNames(databaseName, tableName, partitionNameAsList)).thenReturn(
        partitionAsList);

    //create other files at the same level as the other partition
    snapshot0Dir = Files.createTempDirectory(tableDir, "snapshot0_");
    partition1InSnapshot0Dir = Files.createTempDirectory(snapshot0Dir, "partition1_");
    partition2InSnapshot0Dir = Files.createTempDirectory(snapshot0Dir, "partition2_");

    File file = File.createTempFile("000000_", null, partitionInSnapshot1Dir.toFile());
    File file1 = File.createTempFile("000000_", null, partition1InSnapshot0Dir.toFile());
    File file2 = File.createTempFile("000000_", null, partition2InSnapshot0Dir.toFile());
    writeContentToFiles(file.toPath(), file1.toPath(), file2.toPath());
  }

  private void writeContentToFiles(Path... files) throws IOException {
    for (Path file : files) {
      byte[] fileContent = "content".getBytes();
      Files.write(file, fileContent);
    }
  }

  private void setUnpartitionedTable() throws IOException {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("file:" + snapshot1Dir.toAbsolutePath().toString());
    table.setSd(sd);
    table.setPartitionKeys(Collections.emptyList());
    //create another directory at the same level as the snapshot directory
    snapshot0Dir = Files.createTempDirectory(tableDir, "snapshot0_");
    File file = File.createTempFile("000000_", null, snapshot0Dir.toFile());
    writeContentToFiles(file.toPath(), file.toPath(), file.toPath());
  }

  private boolean assertBytesLogged(int bytes) {
    for (ILoggingEvent event : TestAppender.events) {
      boolean messageIsInLogs = event.getFormattedMessage().contains("bytes: " + bytes);
      if (messageIsInLogs) {
        return true;
      }
    }
    return false;
  }
}
