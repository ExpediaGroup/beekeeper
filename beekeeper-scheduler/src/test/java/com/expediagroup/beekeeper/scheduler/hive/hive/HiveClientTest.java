/**
 * Copyright (C) 2019-2025 Expedia, Inc.
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
package com.expediagroup.beekeeper.scheduler.hive.hive;

import static java.util.Collections.emptyMap;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expediagroup.beekeeper.scheduler.hive.HiveClient;
import com.expediagroup.beekeeper.scheduler.hive.PartitionInfo;
import com.expediagroup.beekeeper.scheduler.hive.PartitionIteratorFactory;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.iterator.PartitionIterator;

@ExtendWith(MockitoExtension.class)
public class HiveClientTest {

  private static final String DATABASE_NAME = "database";
  private static final String TABLE_NAME = "table";
  private static final short NO_LIMIT = (short) -1;
  private static final String PARTITION_NAME = "event_date=2024-01-01/event_hour=1";
  private static final String PARTITION_PATH = "path/" + PARTITION_NAME;

  private @Mock CloseableMetaStoreClient metaStoreClient;
  private @Mock Table table;
  private @Mock FieldSchema eventDatePartitionKey;
  private @Mock FieldSchema eventHourPartitionKey;
  private @Mock Partition partition;
  private @Mock Partition partition2;
  private @Mock Partition partition3;
  private @Mock StorageDescriptor storageDescriptor;
  private @Mock StorageDescriptor storageDescriptor2;
  private @Mock StorageDescriptor storageDescriptor3;
  private @Mock PartitionIteratorFactory partitionIteratorFactory;
  private @Mock PartitionIterator partitionIterator;

  public HiveClient hiveClient;

  @BeforeEach
  public void setup() throws TException {
    hiveClient = new HiveClient(metaStoreClient, partitionIteratorFactory);
  }

  @Test
  public void retrievePartitionAndPaths() throws TException {
    List<String> partitionNames = List.of(PARTITION_NAME);

    when(table.getDbName()).thenReturn(DATABASE_NAME);
    when(table.getTableName()).thenReturn(TABLE_NAME);
    when(table.getPartitionKeys()).thenReturn(List.of(eventDatePartitionKey, eventHourPartitionKey));
    when(eventDatePartitionKey.getName()).thenReturn("EVENT_DATE");
    when(eventHourPartitionKey.getName()).thenReturn("event_hour");

    when(metaStoreClient.listPartitionNames(DATABASE_NAME, TABLE_NAME, NO_LIMIT)).thenReturn(partitionNames);
    when(metaStoreClient.getPartitionsByNames(DATABASE_NAME, TABLE_NAME, partitionNames)).thenReturn(
        List.of(partition));
    when(metaStoreClient.getTable(DATABASE_NAME, TABLE_NAME)).thenReturn(table);
    partitionIterator = new PartitionIteratorFactory().newInstance(metaStoreClient, table);
    when(partitionIteratorFactory.newInstance(metaStoreClient, table)).thenReturn(partitionIterator);

    when(partition.getValues()).thenReturn(List.of("2024-01-01", "1"));
    when(partition.getSd()).thenReturn(storageDescriptor);
    when(storageDescriptor.getLocation()).thenReturn(PARTITION_PATH);
    when(partition.getCreateTime()).thenReturn(1234567890);

    Map<String, PartitionInfo> tablePartitionsInfo = hiveClient.getTablePartitionsInfo(DATABASE_NAME, TABLE_NAME);
    assertThat(tablePartitionsInfo.get(PARTITION_NAME).getPath()).isEqualTo(PARTITION_PATH);
    assertThat(tablePartitionsInfo.get(PARTITION_NAME).getCreateTime())
        .isEqualTo(LocalDateTime.ofInstant(Instant.ofEpochSecond(1234567890), ZoneId.systemDefault()));
  }

  @Test
  public void typicalRetrieveMultiLevelPartitionsAndPaths() throws TException {
    List<String> partitionNames = List.of(PARTITION_NAME, "event_date=2024-01-01/event_hour=2",
        "event_date=2024-01-01/event_hour=3");

    when(table.getDbName()).thenReturn(DATABASE_NAME);
    when(table.getTableName()).thenReturn(TABLE_NAME);
    when(table.getPartitionKeys()).thenReturn(List.of(eventDatePartitionKey, eventHourPartitionKey));
    when(eventDatePartitionKey.getName()).thenReturn("event_date");
    when(eventHourPartitionKey.getName()).thenReturn("event_hour");

    when(metaStoreClient.listPartitionNames(DATABASE_NAME, TABLE_NAME, NO_LIMIT)).thenReturn(partitionNames);
    when(metaStoreClient.getPartitionsByNames(DATABASE_NAME, TABLE_NAME, partitionNames)).thenReturn(
        List.of(partition, partition2, partition3));
    when(metaStoreClient.getTable(DATABASE_NAME, TABLE_NAME)).thenReturn(table);
    partitionIterator = new PartitionIteratorFactory().newInstance(metaStoreClient, table);
    when(partitionIteratorFactory.newInstance(metaStoreClient, table)).thenReturn(partitionIterator);

    when(partition.getValues()).thenReturn(List.of("2024-01-01", "1"));
    when(partition.getSd()).thenReturn(storageDescriptor);
    when(partition2.getValues()).thenReturn(List.of("2024-01-01", "2"));
    when(partition2.getSd()).thenReturn(storageDescriptor2);
    when(partition3.getValues()).thenReturn(List.of("2024-01-01", "3"));
    when(partition3.getSd()).thenReturn(storageDescriptor3);

    String partition2Name = "event_date=2024-01-01/event_hour=2";
    String partition23Name = "event_date=2024-01-01/event_hour=3";
    String partition2Path = "path/event_date=2024-01-01/event_hour=2";
    String partition3Path = "path/event_date=2024-01-01/event_hour=3";
    when(storageDescriptor.getLocation()).thenReturn(PARTITION_PATH);
    when(storageDescriptor2.getLocation()).thenReturn(partition2Path);
    when(storageDescriptor3.getLocation()).thenReturn(partition3Path);

    when(partition.getCreateTime()).thenReturn(1234567890);
    when(partition2.getCreateTime()).thenReturn(1234567891);
    when(partition3.getCreateTime()).thenReturn(1234567892);

    Map<String, PartitionInfo> tablePartitionsInfo = hiveClient.getTablePartitionsInfo(DATABASE_NAME, TABLE_NAME);
    
    assertThat(tablePartitionsInfo.get(PARTITION_NAME).getPath()).isEqualTo(PARTITION_PATH);
    assertThat(tablePartitionsInfo.get(partition2Name).getPath()).isEqualTo(partition2Path);
    assertThat(tablePartitionsInfo.get(partition23Name).getPath()).isEqualTo(partition3Path);

    assertThat(tablePartitionsInfo.get(PARTITION_NAME).getCreateTime())
        .isEqualTo(LocalDateTime.ofInstant(Instant.ofEpochSecond(1234567890), ZoneId.systemDefault()));
    assertThat(tablePartitionsInfo.get(partition2Name).getCreateTime())
        .isEqualTo(LocalDateTime.ofInstant(Instant.ofEpochSecond(1234567891), ZoneId.systemDefault()));
    assertThat(tablePartitionsInfo.get(partition23Name).getCreateTime())
        .isEqualTo(LocalDateTime.ofInstant(Instant.ofEpochSecond(1234567892), ZoneId.systemDefault()));
  }

  @Test
  public void throwsExceptionOnTableRetrieval() throws TException {
    when(metaStoreClient.getTable(DATABASE_NAME, TABLE_NAME)).thenThrow(TException.class);

    Map<String, PartitionInfo> partitionInfo = hiveClient.getTablePartitionsInfo(DATABASE_NAME, TABLE_NAME);
    assertThat(partitionInfo).isEqualTo(emptyMap());
  }
  
  @Test
  public void partitionInfoWithMissingCreateTime() throws TException {
    when(metaStoreClient.getTable(DATABASE_NAME, TABLE_NAME)).thenReturn(table);
    when(table.getPartitionKeys()).thenReturn(List.of(eventDatePartitionKey, eventHourPartitionKey));
    when(eventDatePartitionKey.getName()).thenReturn("event_date");
    when(eventHourPartitionKey.getName()).thenReturn("event_hour");
    when(partitionIteratorFactory.newInstance(metaStoreClient, table)).thenReturn(partitionIterator);
    when(partitionIterator.hasNext()).thenReturn(true, false);
    when(partitionIterator.next()).thenReturn(partition);
    when(partition.getValues()).thenReturn(List.of("2024-01-01", "1"));
    when(partition.getSd()).thenReturn(storageDescriptor);
    when(storageDescriptor.getLocation()).thenReturn(PARTITION_PATH);
    when(partition.getCreateTime()).thenReturn(0);

    LocalDateTime beforeTest = LocalDateTime.now();
    
    Map<String, PartitionInfo> tablePartitionsInfo = hiveClient.getTablePartitionsInfo(DATABASE_NAME, TABLE_NAME);

    LocalDateTime afterTest = LocalDateTime.now();
    
    assertThat(tablePartitionsInfo.get(PARTITION_NAME).getPath()).isEqualTo(PARTITION_PATH);
    LocalDateTime createTime = tablePartitionsInfo.get(PARTITION_NAME).getCreateTime();
    assertThat(createTime).isNotNull();
    assertThat(createTime).isAfterOrEqualTo(beforeTest);
    assertThat(createTime).isBeforeOrEqualTo(afterTest);
  }
  
  @Test
  public void partitionInfoWithNegativeCreateTime() throws TException {
    when(metaStoreClient.getTable(DATABASE_NAME, TABLE_NAME)).thenReturn(table);
    when(table.getPartitionKeys()).thenReturn(List.of(eventDatePartitionKey, eventHourPartitionKey));
    when(eventDatePartitionKey.getName()).thenReturn("event_date");
    when(eventHourPartitionKey.getName()).thenReturn("event_hour");
    when(partitionIteratorFactory.newInstance(metaStoreClient, table)).thenReturn(partitionIterator);
    when(partitionIterator.hasNext()).thenReturn(true, false);
    when(partitionIterator.next()).thenReturn(partition);
    when(partition.getValues()).thenReturn(List.of("2024-01-01", "1"));
    when(partition.getSd()).thenReturn(storageDescriptor);
    when(storageDescriptor.getLocation()).thenReturn(PARTITION_PATH);
    when(partition.getCreateTime()).thenReturn(-1);

    LocalDateTime beforeTest = LocalDateTime.now();
    
    Map<String, PartitionInfo> tablePartitionsInfo = hiveClient.getTablePartitionsInfo(DATABASE_NAME, TABLE_NAME);

    LocalDateTime afterTest = LocalDateTime.now();
    
    assertThat(tablePartitionsInfo.get(PARTITION_NAME).getPath()).isEqualTo(PARTITION_PATH);
    LocalDateTime createTime = tablePartitionsInfo.get(PARTITION_NAME).getCreateTime();
    assertThat(createTime).isNotNull();
    assertThat(createTime).isAfterOrEqualTo(beforeTest);
    assertThat(createTime).isBeforeOrEqualTo(afterTest);
  }
}
