package com.expediagroup.beekeeper.cleanup.path.hive;


import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;
import org.junit.jupiter.api.Test;


import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class HivePathCleanerTest {

    private static final String HMS_DB = "foo_db";
    private static final String HMS_TABLE = "foobar";

    @Mock public HiveClient hiveClient;

    @Test public void shouldExpireTableAfterTimeUp() {
        HivePathCleaner pathCleaner = new HivePathCleaner(hiveClient);
        EntityHousekeepingPath housekeepingPath = new EntityHousekeepingPath();
        housekeepingPath.setPath("s3://foo/bar.baz");
        housekeepingPath.setCleanupTimestamp(LocalDateTime.MIN);
        housekeepingPath.setDatabaseName(HMS_DB);
        housekeepingPath.setTableName(HMS_TABLE);
        when(hiveClient.dropTable(HMS_DB, HMS_TABLE)).thenReturn(true);
        assertTrue(pathCleaner.cleanupPath(housekeepingPath));
    }

    @Test public void shouldNotExpireTableBeforeTimeUp() {
        HivePathCleaner pathCleaner = new HivePathCleaner(hiveClient);
        EntityHousekeepingPath housekeepingPath = new EntityHousekeepingPath();
        housekeepingPath.setPath("s3://foo/bar.baz");
        housekeepingPath.setCleanupTimestamp(LocalDateTime.MAX);
        housekeepingPath.setDatabaseName(HMS_DB);
        housekeepingPath.setTableName(HMS_TABLE);
        assertFalse(pathCleaner.cleanupPath(housekeepingPath));
        verify(hiveClient, never()).dropTable(HMS_DB, HMS_TABLE);
    }

    @Test public void shouldExpirePartitionAfterTimeUp() {
        // TODO: Determine what makes a housekeeping path a partition
        HivePathCleaner pathCleaner = new HivePathCleaner(hiveClient);
        EntityHousekeepingPath housekeepingPath = new EntityHousekeepingPath();
        housekeepingPath.setPath("s3://foo/bar.baz");
        housekeepingPath.setCleanupTimestamp(LocalDateTime.MIN);
        housekeepingPath.setDatabaseName(HMS_DB);
        housekeepingPath.setTableName(HMS_TABLE);
        fail();
    }

    @Test public void shouldNotPartitionTableBeforeTimeUp() {
        // TODO: Determine what makes a housekeeping path a partition
        HivePathCleaner pathCleaner = new HivePathCleaner(hiveClient);
        EntityHousekeepingPath housekeepingPath = new EntityHousekeepingPath();
        housekeepingPath.setPath("s3://foo/bar.baz");
        housekeepingPath.setCleanupTimestamp(LocalDateTime.MAX);
        housekeepingPath.setDatabaseName(HMS_DB);
        housekeepingPath.setTableName(HMS_TABLE);
        fail();
    }
}
