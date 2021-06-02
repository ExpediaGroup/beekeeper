/**
 * Copyright (C) 2019-2021 Expedia, Inc.
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
package com.expediagroup.beekeeper.path.cleanup.context;

import java.util.List;

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
import com.expediagroup.beekeeper.cleanup.monitoring.BytesDeletedReporter;
import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.cleanup.service.CleanupService;
import com.expediagroup.beekeeper.path.cleanup.handler.GenericPathHandler;
import com.expediagroup.beekeeper.path.cleanup.service.PagingPathCleanupService;

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
  AmazonS3 amazonS3Test() {
    String s3Endpoint = System.getProperty("aws.s3.endpoint");
    String region = System.getProperty("aws.region");

    EndpointConfiguration endpointConfiguration = new EndpointConfiguration(s3Endpoint, region);
    return AmazonS3ClientBuilder.standard()
        .withEndpointConfiguration(endpointConfiguration)
        .build();
  }

  @Bean
  BytesDeletedReporter bytesDeletedReporter(
      MeterRegistry meterRegistry,
      @Value("${properties.dry-run-enabled}") boolean dryRunEnabled) {
    return new BytesDeletedReporter(meterRegistry, dryRunEnabled);
  }

  @Bean
  public S3Client s3Client(AmazonS3 amazonS3, @Value("${properties.dry-run-enabled}") boolean dryRunEnabled) {
    return new S3Client(amazonS3, dryRunEnabled);
  }

  @Bean(name = "s3PathCleaner")
  PathCleaner pathCleaner(
      S3Client s3Client,
      BytesDeletedReporter bytesDeletedReporter) {
    return new S3PathCleaner(s3Client, new S3SentinelFilesCleaner(s3Client), bytesDeletedReporter);
  }

  @Bean
  CleanupService cleanupService(
      List<GenericPathHandler> pathHandlers,
      @Value("${properties.cleanup-page-size}") int pageSize,
      @Value("${properties.dry-run-enabled}") boolean dryRunEnabled) {
    return new PagingPathCleanupService(pathHandlers, pageSize, dryRunEnabled);
  }
}
