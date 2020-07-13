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
package com.expediagroup.beekeeper.metadata.cleanup.context;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expediagroup.beekeeper.core.hive.HiveClient;
import com.expediagroup.beekeeper.core.hive.HiveMetadataCleaner;
import com.expediagroup.beekeeper.core.metadata.MetadataCleaner;
import com.expediagroup.beekeeper.core.monitoring.DeletedMetadataReporter;
import com.expediagroup.beekeeper.core.path.PathCleaner;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;
import com.expediagroup.beekeeper.metadata.cleanup.service.MetadataCleanupService;
import com.expediagroup.beekeeper.metadata.cleanup.service.PagingMetadataCleanupService;

@ExtendWith(MockitoExtension.class)
public class CommonBeansTest {

  private Boolean dryRunEnabled = false;
  private final CommonBeans commonBeans = new CommonBeans();
  private @Mock HousekeepingPathRepository repository;
  private @Mock PathCleaner pathCleaner;
  private @Mock DeletedMetadataReporter deletedMetadataReporter;
  private @Mock HiveConf hiveConf;
  private final String metastoreUri = "thrift://localhost:1234";

  // check metastore client supplier
  // check hive client
  // check metadata cleaner
  // check clean up service

  // TODO
  // aws stuff ????

  @Test
  public void typicalMetastoreClient() {

  }

  @Test
  public void hiveClient() {

  }

  @Test
  public void verifyHiveMetadataCleaner() {

    HiveClient hiveClient = Mockito.mock(HiveClient.class);
        // commonBeans.hiveClient(commonBeans.metaStoreClientSupplier(metaStoreClientFactory, hiveConf), dryRunEnabled);

    MetadataCleaner metadataCleaner = commonBeans.metadataCleaner(hiveClient, deletedMetadataReporter, dryRunEnabled);
    assertThat(metadataCleaner).isInstanceOf(HiveMetadataCleaner.class);
  }

  @Test
  public void cleanupService() {
    MetadataCleanupService cleanupService = commonBeans.cleanupService(Collections.emptyList(), 2, dryRunEnabled);
    assertThat(cleanupService).isInstanceOf(PagingMetadataCleanupService.class);
  }

}
