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
package com.expediagroup.beekeeper.cleanup.context;

import com.expediagroup.beekeeper.cleanup.path.hive.HiveClient;
import com.expediagroup.beekeeper.cleanup.path.hive.HivePathCleaner;
import com.expediagroup.beekeeper.core.model.LifeCycleEventType;
//import org.apache.hadoop.hive.conf.HiveConf;
//import org.apache.hadoop.hive.metastore.HiveMetaException;
//import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
//import org.apache.hadoop.hive.metastore.api.MetaException;
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

import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.cleanup.path.aws.S3BytesDeletedReporter;
import com.expediagroup.beekeeper.cleanup.path.aws.S3Client;
import com.expediagroup.beekeeper.cleanup.path.aws.S3PathCleaner;
import com.expediagroup.beekeeper.cleanup.path.aws.S3SentinelFilesCleaner;
import com.expediagroup.beekeeper.cleanup.service.CleanupService;
import com.expediagroup.beekeeper.cleanup.service.PagingCleanupService;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;

import java.util.EnumMap;

@Configuration
@EnableScheduling
@ComponentScan({ "com.expediagroup.beekeeper.core", "com.expediagroup.beekeeper.cleanup" })
@EntityScan(basePackages = { "com.expediagroup.beekeeper.core.model" })
@EnableJpaRepositories(basePackages = { "com.expediagroup.beekeeper.core.repository" })
public class CommonBeans {

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

    EndpointConfiguration endpointConfiguration = new EndpointConfiguration(s3Endpoint, region);
    return AmazonS3ClientBuilder.standard()
        .withEndpointConfiguration(endpointConfiguration)
        .build();
  }

  @Bean
  public S3Client s3Client(AmazonS3 amazonS3, @Value("${properties.dry-run-enabled}") boolean dryRunEnabled) {
    return new S3Client(amazonS3, dryRunEnabled);
  }

  @Bean
  public S3BytesDeletedReporter s3BytesDeletedReporter(S3Client s3Client, MeterRegistry meterRegistry,
      @Value("${properties.dry-run-enabled}") boolean dryRunEnabled) {
    return new S3BytesDeletedReporter(s3Client, meterRegistry, dryRunEnabled);
  }

  @Bean
  public S3PathCleaner s3PathCleaner(S3Client s3Client, S3BytesDeletedReporter s3BytesDeletedReporter) {
    return new S3PathCleaner(s3Client, new S3SentinelFilesCleaner(s3Client), s3BytesDeletedReporter);
  }

//  @Bean
//  public HiveClient hiveClient(
//      @Value("${properties.dry-run-enabled}") boolean dryRunEnabled
//  ) throws MetaException {
//    HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(new HiveConf());
//    return new HiveClient(hiveMetaStoreClient, dryRunEnabled);
//  }

//  @Bean
//  public HivePathCleaner hivePathCleaner(
//      HiveClient hiveClient
//  ) {
//    return new HivePathCleaner(hiveClient);
//  }

  @Bean
  public EnumMap<LifeCycleEventType, PathCleaner> pathCleanerMap(
      S3PathCleaner s3PathCleaner,
      HivePathCleaner hivePathCleaner
  ) {
    EnumMap<LifeCycleEventType, PathCleaner> pcMap = new EnumMap<>(LifeCycleEventType.class);
    pcMap.put(LifeCycleEventType.UNREFERENCED, s3PathCleaner);
//    pcMap.put(LifeCycleEventType.EXPIRED, hivePathCleaner);
    return pcMap;
  }

  @Bean
  public CleanupService cleanupService(
       HousekeepingPathRepository repository,
       EnumMap<LifeCycleEventType, PathCleaner> pathCleanerMap,
       @Value("${properties.cleanup-page-size}") int pageSize,
       @Value("${properties.dry-run-enabled}") boolean dryRunEnabled
  ) {
    return new PagingCleanupService(repository, pathCleanerMap, pageSize, dryRunEnabled);
  }
}
