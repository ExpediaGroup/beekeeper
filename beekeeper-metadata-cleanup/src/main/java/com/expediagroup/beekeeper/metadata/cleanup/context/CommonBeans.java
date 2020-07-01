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

import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.expediagroup.beekeeper.metadata.cleanup.handler.GenericMetadataHandler;
import com.expediagroup.beekeeper.metadata.cleanup.hive.HiveClient;
import com.expediagroup.beekeeper.metadata.cleanup.hive.HiveMetadataCleaner;
import com.expediagroup.beekeeper.metadata.cleanup.monitoring.DeletedMetadataReporter;
import com.expediagroup.beekeeper.metadata.cleanup.service.PagingMetadataCleanupService;
import com.expediagroup.beekeeper.metadata.cleanup.table.MetadataCleaner;
import com.expediagroup.beekeeper.metadata.cleanup.service.MetadataCleanupService;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@Configuration
@EnableScheduling
@ComponentScan({ "com.expediagroup.beekeeper.core", "com.expediagroup.beekeeper.cleanup" })
@EntityScan(basePackages = { "com.expediagroup.beekeeper.core.model" })
@EnableJpaRepositories(basePackages = { "com.expediagroup.beekeeper.core.repository" })
public class CommonBeans {

  @Bean
  @Profile("default")
  CloseableMetaStoreClient metaStoreClient() {

    // TODO
    // set up the metastore client

    return null;
  }

  @Bean
  public HiveClient hiveClient(
      CloseableMetaStoreClient client,
      @Value("${properties.dry-run-enabled}") boolean dryRunEnabled) {
    return new HiveClient(client, dryRunEnabled);
  }

  @Bean(name = "hiveTableCleaner")
  MetadataCleaner TableCleaner(HiveClient hiveClient, DeletedMetadataReporter tablesDeletedReporter) {
    return new HiveMetadataCleaner(hiveClient, tablesDeletedReporter);
  }

  @Bean
  MetadataCleanupService cleanupService(
      List<GenericMetadataHandler> pathHandlers,
      @Value("${properties.cleanup-page-size}") int pageSize,
      @Value("${properties.dry-run-enabled}") boolean dryRunEnabled
  ) {
    return new PagingMetadataCleanupService(pathHandlers, pageSize, dryRunEnabled);
  }
}
