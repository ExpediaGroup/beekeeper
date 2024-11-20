/**
 * Copyright (C) 2019-2024 Expedia, Inc.
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
package com.expediagroup.beekeeper.cleanup.hive;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expediagroup.beekeeper.cleanup.monitoring.DeletedMetadataReporter;
import com.expediagroup.beekeeper.cleanup.validation.IcebergValidator;
import com.expediagroup.beekeeper.core.config.MetadataType;
import com.expediagroup.beekeeper.core.error.BeekeeperIcebergException;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;

@ExtendWith(MockitoExtension.class)
public class HiveMetadataCleanerTest {

  private @Mock HousekeepingMetadata housekeepingMetadata;
  private @Mock DeletedMetadataReporter deletedMetadataReporter;
  private @Mock HiveClient hiveClient;
  private @Mock IcebergValidator icebergValidator;

  private HiveMetadataCleaner cleaner;
  private static final String DATABASE = "database";
  private static final String TABLE_NAME = "tableName";
  private static final String PARTITION_NAME = "event_date=2020-01-01/event_hour=0/event_type=A";

  @BeforeEach
  public void init() {
    cleaner = new HiveMetadataCleaner(deletedMetadataReporter, icebergValidator);
  }

  @Test
  public void typicalDropTable() {
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);

    cleaner.dropTable(housekeepingMetadata, hiveClient);
    // Verify that the IcebergValidator is called
    verify(icebergValidator).throwExceptionIfIceberg(DATABASE, TABLE_NAME);
    verify(hiveClient).dropTable(DATABASE, TABLE_NAME);
    verify(deletedMetadataReporter).reportTaggable(housekeepingMetadata, MetadataType.HIVE_TABLE);
  }

  @Test
  public void typicalDropPartition() {
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(housekeepingMetadata.getPartitionName()).thenReturn(PARTITION_NAME);
    when(hiveClient.dropPartition(DATABASE, TABLE_NAME, PARTITION_NAME)).thenReturn(true);

    cleaner.dropPartition(housekeepingMetadata, hiveClient);

    verify(icebergValidator).throwExceptionIfIceberg(DATABASE, TABLE_NAME);
    verify(hiveClient).dropPartition(DATABASE, TABLE_NAME, PARTITION_NAME);
    verify(deletedMetadataReporter).reportTaggable(housekeepingMetadata, MetadataType.HIVE_PARTITION);
  }

  @Test
  public void dontReportWhenPartitionNotDropped() {
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(housekeepingMetadata.getPartitionName()).thenReturn(PARTITION_NAME);
    when(hiveClient.dropPartition(DATABASE, TABLE_NAME, PARTITION_NAME)).thenReturn(false);

    cleaner.dropPartition(housekeepingMetadata, hiveClient);
    verify(deletedMetadataReporter, never()).reportTaggable(housekeepingMetadata, MetadataType.HIVE_PARTITION);
  }

  @Test
  public void tableExists() {
    cleaner.tableExists(hiveClient, DATABASE, TABLE_NAME);
    verify(hiveClient).tableExists(DATABASE, TABLE_NAME);
  }

  @Test
  public void dropTableWhenIcebergTable() {
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    doThrow(new BeekeeperIcebergException("Iceberg table"))
        .when(icebergValidator).throwExceptionIfIceberg(DATABASE, TABLE_NAME);

    assertThrows(
        BeekeeperIcebergException.class,
        () -> cleaner.dropTable(housekeepingMetadata, hiveClient)
    );

    // Verify that dropTable was not called on hiveClient
    verify(hiveClient, never()).dropTable(DATABASE, TABLE_NAME);
    verify(deletedMetadataReporter, never()).reportTaggable(housekeepingMetadata, MetadataType.HIVE_TABLE);
  }

  @Test
  public void dropPartitionWhenIcebergTable() {
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    doThrow(new BeekeeperIcebergException("Iceberg table"))
        .when(icebergValidator).throwExceptionIfIceberg(DATABASE, TABLE_NAME);

    assertThrows(
        BeekeeperIcebergException.class,
        () -> cleaner.dropPartition(housekeepingMetadata, hiveClient)
    );

    verify(hiveClient, never()).dropPartition(DATABASE, TABLE_NAME, PARTITION_NAME);
    verify(deletedMetadataReporter, never()).reportTaggable(housekeepingMetadata, MetadataType.HIVE_PARTITION);
  }
}
