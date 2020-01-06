/**
 * Copyright (C) 2019 Expedia, Inc.
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
package com.expediagroup.beekeeper.cleanup.path.hive;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;

@ExtendWith(MockitoExtension.class)
public class HivePathCleanerTest {

  private static final String HMS_DB = "foo_db";
  private static final String HMS_TABLE = "foobar";

  @Mock private HiveClient hiveClient;

  @Test
  public void shouldExpireTableAfterTimeUp() {
    HivePathCleaner pathCleaner = new HivePathCleaner(hiveClient);
    EntityHousekeepingPath housekeepingPath = new EntityHousekeepingPath();
    housekeepingPath.setPath("s3://foo/bar.baz");
    housekeepingPath.setCleanupTimestamp(LocalDateTime.MIN);
    housekeepingPath.setDatabaseName(HMS_DB);
    housekeepingPath.setTableName(HMS_TABLE);
    when(hiveClient.dropTable(HMS_DB, HMS_TABLE)).thenReturn(true);
    assertTrue(pathCleaner.cleanupPath(housekeepingPath));
  }

  @Test
  public void shouldNotExpireTableBeforeTimeUp() {
    HivePathCleaner pathCleaner = new HivePathCleaner(hiveClient);
    EntityHousekeepingPath housekeepingPath = new EntityHousekeepingPath();
    housekeepingPath.setPath("s3://foo/bar.baz");
    housekeepingPath.setCleanupTimestamp(LocalDateTime.MAX);
    housekeepingPath.setDatabaseName(HMS_DB);
    housekeepingPath.setTableName(HMS_TABLE);
    assertFalse(pathCleaner.cleanupPath(housekeepingPath));
    verify(hiveClient, never()).dropTable(HMS_DB, HMS_TABLE);
  }

//    @Test public void shouldExpirePartitionAfterTimeUp() {
//        HivePathCleaner pathCleaner = new HivePathCleaner(hiveClient);
//        EntityHousekeepingPath housekeepingPath = new EntityHousekeepingPath();
//        housekeepingPath.setPath("s3://foo/bar.baz");
//        housekeepingPath.setCleanupTimestamp(LocalDateTime.MIN);
//        housekeepingPath.setDatabaseName(HMS_DB);
//        housekeepingPath.setTableName(HMS_TABLE);
//        fail();
//    }
//
//    @Test public void shouldNotPartitionTableBeforeTimeUp() {
//        HivePathCleaner pathCleaner = new HivePathCleaner(hiveClient);
//        EntityHousekeepingPath housekeepingPath = new EntityHousekeepingPath();
//        housekeepingPath.setPath("s3://foo/bar.baz");
//        housekeepingPath.setCleanupTimestamp(LocalDateTime.MAX);
//        housekeepingPath.setDatabaseName(HMS_DB);
//        housekeepingPath.setTableName(HMS_TABLE);
//        fail();
//    }
}
