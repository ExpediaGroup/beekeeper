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
package com.expediagroup.beekeeper.metadata.cleanup.context;

import java.util.List;
import java.util.function.Supplier;

import org.apache.hadoop.hive.conf.HiveConf;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

import io.micrometer.core.instrument.MeterRegistry;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import com.expediagroup.beekeeper.cleanup.aws.S3Client;
import com.expediagroup.beekeeper.cleanup.aws.S3PathCleaner;
import com.expediagroup.beekeeper.cleanup.aws.S3SentinelFilesCleaner;
import com.expediagroup.beekeeper.cleanup.hive.HiveClientFactory;
import com.expediagroup.beekeeper.cleanup.hive.HiveMetadataCleaner;
import com.expediagroup.beekeeper.cleanup.metadata.CleanerClientFactory;
import com.expediagroup.beekeeper.cleanup.metadata.MetadataCleaner;
import com.expediagroup.beekeeper.cleanup.monitoring.BytesDeletedReporter;
import com.expediagroup.beekeeper.cleanup.monitoring.DeletedMetadataReporter;
import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.cleanup.service.CleanupService;
import com.expediagroup.beekeeper.cleanup.service.DisableTablesService;
import com.expediagroup.beekeeper.cleanup.service.RepositoryCleanupService;
import com.expediagroup.beekeeper.cleanup.validation.IcebergValidator;
import com.expediagroup.beekeeper.core.repository.BeekeeperHistoryRepository;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;
import com.expediagroup.beekeeper.core.service.BeekeeperHistoryService;
import com.expediagroup.beekeeper.metadata.cleanup.handler.ExpiredMetadataHandler;
import com.expediagroup.beekeeper.metadata.cleanup.handler.MetadataHandler;
import com.expediagroup.beekeeper.metadata.cleanup.service.MetadataDisableTablesService;
import com.expediagroup.beekeeper.metadata.cleanup.service.MetadataRepositoryCleanupService;
import com.expediagroup.beekeeper.metadata.cleanup.service.PagingMetadataCleanupService;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.closeable.CloseableMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.supplier.HiveMetaStoreClientSupplier;

@Configuration
@EnableScheduling
@ComponentScan({ "com.expediagroup.beekeeper.core", "com.expediagroup.beekeeper.cleanup" })
@EntityScan(basePackages = { "com.expediagroup.beekeeper.core.model" })
@EnableJpaRepositories(basePackages = { "com.expediagroup.beekeeper.core.repository" })
public class CommonBeans {

  @Bean
  public HiveConf hiveConf(@Value("${properties.metastore-uri}") String metastoreUri) {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUri);
    return conf;
  }

  @Bean
  public CloseableMetaStoreClientFactory metaStoreClientFactory() {
    return new CloseableMetaStoreClientFactory();
  }

  @Bean
  public Supplier<CloseableMetaStoreClient> metaStoreClientSupplier(
      CloseableMetaStoreClientFactory metaStoreClientFactory, HiveConf hiveConf) {
    String name = "beekeeper-metadata-cleanup";
    return new HiveMetaStoreClientSupplier(metaStoreClientFactory, hiveConf, name);
  }

  @Bean(name = "hiveClientFactory")
  public CleanerClientFactory clientFactory(
      Supplier<CloseableMetaStoreClient> metaStoreClientSupplier,
      @Value("${properties.dry-run-enabled}") boolean dryRunEnabled) {
    return new HiveClientFactory(metaStoreClientSupplier, dryRunEnabled);
  }

  @Bean
  public IcebergValidator icebergValidator(CleanerClientFactory clientFactory) {
    return new IcebergValidator(clientFactory);
  }

  @Bean
  public DeletedMetadataReporter deletedMetadataReporter(
      MeterRegistry meterRegistry,
      @Value("${properties.dry-run-enabled}") boolean dryRunEnabled) {
    return new DeletedMetadataReporter(meterRegistry, dryRunEnabled);
  }

  @Bean(name = "hiveTableCleaner")
  public MetadataCleaner metadataCleaner(
      DeletedMetadataReporter deletedMetadataReporter, IcebergValidator icebergValidator) {
    return new HiveMetadataCleaner(deletedMetadataReporter, icebergValidator);
  }

  @Bean
  @Profile("default")
  public AmazonS3 amazonS3() {
    return AmazonS3ClientBuilder.defaultClient();
  }

  @Bean
  @Profile("test")
  public AmazonS3 amazonS3Test() {
    String s3Endpoint = System.getProperty("aws.s3.endpoint");
    String region = System.getProperty("aws.region");

    return AmazonS3ClientBuilder
        .standard()
        .withEndpointConfiguration(new EndpointConfiguration(s3Endpoint, region))
        .build();
  }

  @Bean
  public BytesDeletedReporter bytesDeletedReporter(
      MeterRegistry meterRegistry,
      @Value("${properties.dry-run-enabled}") boolean dryRunEnabled) {
    return new BytesDeletedReporter(meterRegistry, dryRunEnabled);
  }

  @Bean
  public S3Client s3Client(AmazonS3 amazonS3, @Value("${properties.dry-run-enabled}") boolean dryRunEnabled) {
    return new S3Client(amazonS3, dryRunEnabled);
  }

  @Bean(name = "s3PathCleaner")
  public PathCleaner pathCleaner(
      S3Client s3Client,
      BytesDeletedReporter bytesDeletedReporter) {
    return new S3PathCleaner(s3Client, new S3SentinelFilesCleaner(s3Client), bytesDeletedReporter);
  }

  @Bean(name = "expiredMetadataHandler")
  public ExpiredMetadataHandler expiredMetadataHandler(
      @Qualifier("hiveClientFactory") CleanerClientFactory cleanerClientFactory,
      HousekeepingMetadataRepository housekeepingMetadataRepository,
      @Qualifier("hiveTableCleaner") MetadataCleaner metadataCleaner,
      @Qualifier("s3PathCleaner") PathCleaner pathCleaner,
      BeekeeperHistoryService beekeeperHistoryService) {
    return new ExpiredMetadataHandler(cleanerClientFactory, housekeepingMetadataRepository, metadataCleaner,
        pathCleaner, beekeeperHistoryService);
  }

  @Bean
  public BeekeeperHistoryService beekeeperHistoryService(BeekeeperHistoryRepository beekeeperHistoryRepository) {
    return new BeekeeperHistoryService(beekeeperHistoryRepository);
  }

  @Bean
  public CleanupService cleanupService(
      List<MetadataHandler> metadataHandlers,
      @Value("${properties.cleanup-page-size}") int pageSize,
      @Value("${properties.dry-run-enabled}") boolean dryRunEnabled) {
    return new PagingMetadataCleanupService(metadataHandlers, pageSize, dryRunEnabled);
  }

  @Bean
  public RepositoryCleanupService repositoryCleanupService(
      HousekeepingMetadataRepository housekeepingMetadataRepository,
      @Value("${properties.old-data-retention-period-days}") int retentionPeriodInDays) {
    return new MetadataRepositoryCleanupService(housekeepingMetadataRepository, retentionPeriodInDays);
  }

  @Bean
  public DisableTablesService disableTablesService(
      HousekeepingMetadataRepository housekeepingMetadataRepository,
      CleanerClientFactory cleanerClientFactory,
      @Value("${properties.dry-run-enabled}") boolean dryRunEnabled) {
    return new MetadataDisableTablesService(cleanerClientFactory, housekeepingMetadataRepository, dryRunEnabled);
  }
}
