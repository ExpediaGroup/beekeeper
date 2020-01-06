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

//import com.hotels.beeju.extensions.HiveMetaStoreJUnitExtension;
public class HiveClientTest {

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
}
