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
package com.expediagroup.beekeeper.cleanup.hive;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
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
    hiveClient.dropTableIfExists(DATABASE, TABLE_NAME);
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

    hiveClient.dropTableIfExists(DATABASE, TABLE_NAME);
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
    hiveClient.dropTableIfExists(DATABASE, TABLE_NAME);
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
      hiveClient.dropTableIfExists(DATABASE, TABLE_NAME);
    });
  }

  @Test
  public void throwsExceptionForDropPartition() throws TException {
    Mockito.doThrow(MetaException.class).when(client).dropPartition(DATABASE, TABLE_NAME, PARTITION_NAME, false);
    assertThrows(BeekeeperException.class, () -> {
      hiveClient.dropPartition(DATABASE, TABLE_NAME, PARTITION_NAME);
    });
  }
}
