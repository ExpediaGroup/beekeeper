package com.expediagroup.beekeeper.cleanup.path.hive;


import com.hotels.beeju.extensions.HiveMetaStoreJUnitExtension;
import com.hotels.beeju.extensions.ThriftHiveMetaStoreJUnitExtension;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.engine.Constants;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HivePathCleanerTest {

    private static final String TARGET_UNPARTITIONED_TABLE = "ct_table_u_copy";
    private static final String TARGET_PARTITIONED_TABLE = "ct_table_p_copy";
    private static final String TARGET_UNPARTITIONED_MANAGED_TABLE = "ct_table_u_managed_copy";
    private static final String TARGET_PARTITIONED_MANAGED_TABLE = "ct_table_p_managed_copy";
    private static final String TARGET_PARTITIONED_VIEW = "ct_view_p_copy";
    private static final String TARGET_UNPARTITIONED_VIEW = "ct_view_u_copy";

//    public @Rule ExpectedSystemExit exit = ExpectedSystemExit.none();
//    public @Rule TemporaryFolder temporaryFolder = new TemporaryFolder();
//    public @Rule DataFolder dataFolder = new ClassDataFolder();
//    public @Rule ThriftHiveMetaStoreJUnitRule sourceCatalog = new ThriftHiveMetaStoreJUnitRule(DATABASE);
//    public @Rule ThriftHiveMetaStoreJUnitRule replicaCatalog = new ThriftHiveMetaStoreJUnitRule(DATABASE);
//    public @Rule ServerSocketRule serverSocketRule = new ServerSocketRule();

    @RegisterExtension
    public HiveMetaStoreJUnitExtension hive = new HiveMetaStoreJUnitExtension("foo_db");

    @Test
    public void example() throws Exception {
        HiveClient classUnderTest = new HiveClient(hive.client(), false);
        List<String> dbs = classUnderTest.getAllDatabases();
        assertEquals(2, dbs.size());
    }

}
