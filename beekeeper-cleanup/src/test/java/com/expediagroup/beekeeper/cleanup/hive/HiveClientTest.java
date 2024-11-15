/**
 * Copyright (C) 2019-2022 Expedia, Inc.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expediagroup.beekeeper.core.error.BeekeeperException;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@ExtendWith(MockitoExtension.class)
public class HiveClientTest {

  private @Mock CloseableMetaStoreClient client;

  private static final String DATABASE = "database";
  private static final String TABLE_NAME = "tableName";
  private static final String PARTITION_NAME = "event_date=2020-01-01/event_hour=0/event_type=A";

  private HiveClient hiveClient;

  @BeforeEach
  public void init() {
    hiveClient = new HiveClient(client, false);
  }

  @Test
  public void typicalDropTable() throws TException {
    hiveClient.dropTable(DATABASE, TABLE_NAME);
    verify(client).dropTable(DATABASE, TABLE_NAME);
  }

  @Test
  public void typicalDropPartition() throws TException {
    boolean result = hiveClient.dropPartition(DATABASE, TABLE_NAME, PARTITION_NAME);
    verify(client).dropPartition(DATABASE, TABLE_NAME, PARTITION_NAME, false);
    assertTrue(result);
  }

  @Test
  public void typicalDropTableDryRun() throws TException {
    hiveClient = new HiveClient(client, true);

    hiveClient.dropTable(DATABASE, TABLE_NAME);
    verify(client, never()).dropTable(DATABASE, TABLE_NAME);
  }

  @Test
  public void typicalDropPartitionDryRun() throws TException {
    hiveClient = new HiveClient(client, true);
    boolean result = hiveClient.dropPartition(DATABASE, TABLE_NAME, PARTITION_NAME);
    verify(client, never()).dropPartition(DATABASE, TABLE_NAME, PARTITION_NAME, false);
    assertTrue(result);
  }

  @Test
  public void dontThrowErrorWhenTableAlreadyDropped() throws TException {
    Mockito.doThrow(NoSuchObjectException.class).when(client).dropTable(DATABASE, TABLE_NAME);
    hiveClient.dropTable(DATABASE, TABLE_NAME);
    verify(client).dropTable(DATABASE, TABLE_NAME);
  }

  @Test
  public void dontThrowErrorWhenPartitionAlreadyDropped() throws TException {
    Mockito
        .doThrow(NoSuchObjectException.class)
        .when(client)
        .dropPartition(DATABASE, TABLE_NAME, PARTITION_NAME, false);

    boolean result = hiveClient.dropPartition(DATABASE, TABLE_NAME, PARTITION_NAME);
    verify(client).dropPartition(DATABASE, TABLE_NAME, PARTITION_NAME, false);
    assertFalse(result);
  }

  @Test
  public void throwsExceptionForDropTable() throws TException {
    Mockito.doThrow(MetaException.class).when(client).dropTable(DATABASE, TABLE_NAME);
    assertThrows(BeekeeperException.class, () -> {
      hiveClient.dropTable(DATABASE, TABLE_NAME);
    });
  }

  @Test
  public void throwsExceptionForDropPartition() throws TException {
    Mockito.doThrow(MetaException.class).when(client).dropPartition(DATABASE, TABLE_NAME, PARTITION_NAME, false);
    assertThrows(BeekeeperException.class, () -> {
      hiveClient.dropPartition(DATABASE, TABLE_NAME, PARTITION_NAME);
    });
  }

  @Test
  public void tableExistsThrowsException() throws TException {
    when(client.tableExists(DATABASE, TABLE_NAME)).thenThrow(new TException());
    assertThrows(BeekeeperException.class, () -> {
      hiveClient.tableExists(DATABASE, TABLE_NAME);
    });
  }

  @Test
  public void getTableProperties() throws TException {
    Table table = new Table();
    Map<String, String> params = new HashMap<>();
    table.setParameters(params);
    when(client.getTable(DATABASE, TABLE_NAME)).thenReturn(table);
    assertEquals(hiveClient.getTableProperties(DATABASE, TABLE_NAME), params);
  }

  @Test
  public void getTablePropertiesNullPropertiesReturnsEmpty() throws TException {
    when(client.getTable(DATABASE, TABLE_NAME)).thenReturn(new Table());
    assertEquals(hiveClient.getTableProperties(DATABASE, TABLE_NAME), new HashMap<>());
  }

  @Test
  public void getTablePropertiesForNonexistentTableReturnsEmpty() throws TException {
    when(client.getTable(DATABASE, TABLE_NAME)).thenThrow(new NoSuchObjectException(""));
    assertEquals(hiveClient.getTableProperties(DATABASE, TABLE_NAME), new HashMap<>());
  }

  @Test
  public void getTablePropertiesThrowsException() throws TException {
    when(client.getTable(DATABASE, TABLE_NAME)).thenThrow(new TException());
    assertThrows(BeekeeperException.class, () -> {
      hiveClient.getTableProperties(DATABASE, TABLE_NAME);
    });
  }

  @Test
  public void getStorageDescriptorPropertiesWithValidOutputFormat() throws TException {
    // create a table with a valid outputFormat
    Table table = new Table();
    StorageDescriptor sd = new StorageDescriptor();
    sd.setOutputFormat("org.apache.iceberg.mr.hive.HiveIcebergOutputFormat");
    table.setSd(sd);

    when(client.getTable(DATABASE, TABLE_NAME)).thenReturn(table);
    // retrieve storage descriptor properties
    Map<String, String> result = hiveClient.getStorageDescriptorProperties(DATABASE, TABLE_NAME);
    // verifying the outputFormat should be correctly retrieved
    Map<String, String> expected = new HashMap<>();
    expected.put("outputFormat", "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat");

    assertEquals(expected, result);
  }

  @Test
  public void getStorageDescriptorPropertiesWithNullOutputFormat() throws TException {
    Table table = new Table();
    StorageDescriptor sd = new StorageDescriptor();
    sd.setOutputFormat(null);
    table.setSd(sd);

    when(client.getTable(DATABASE, TABLE_NAME)).thenReturn(table);

    Map<String, String> result = hiveClient.getStorageDescriptorProperties(DATABASE, TABLE_NAME);
    Map<String, String> expected = new HashMap<>();
    expected.put("outputFormat", null);

    assertEquals(expected, result);
  }
}
