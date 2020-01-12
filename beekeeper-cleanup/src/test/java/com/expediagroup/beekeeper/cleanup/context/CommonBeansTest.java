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
package com.expediagroup.beekeeper.cleanup.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.net.URL;
import java.util.Collections;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.graphite.GraphiteMeterRegistry;

import com.amazonaws.services.s3.AmazonS3;

import com.expediagroup.beekeeper.cleanup.monitoring.BytesDeletedReporter;
import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.cleanup.path.aws.S3Client;
import com.expediagroup.beekeeper.cleanup.path.aws.S3PathCleaner;
import com.expediagroup.beekeeper.cleanup.service.CleanupService;
import com.expediagroup.beekeeper.cleanup.service.PagingCleanupService;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;

@ExtendWith(MockitoExtension.class)
class CommonBeansTest {

  private static final String AWS_S3_ENDPOINT_PROPERTY = "aws.s3.endpoint";
  private static final String AWS_REGION_PROPERTY = "aws.region";
  private static final String REGION = "us-west-2";
  private static final String AWS_ENDPOINT = String.join(".", "s3", REGION, "amazonaws.com");
  private static final String ENDPOINT = "endpoint";
  private static final String BUCKET = "bucket";
  private static final String KEY = "key";
  private final CommonBeans commonBeans = new CommonBeans();
  private @Mock HousekeepingPathRepository repository;
  private @Mock PathCleaner pathCleaner;

  @AfterAll
  static void teardown() {
    System.clearProperty(AWS_REGION_PROPERTY);
    System.clearProperty(AWS_S3_ENDPOINT_PROPERTY);
  }

  @BeforeEach
  void setUp() {
    System.setProperty(AWS_REGION_PROPERTY, REGION);
    System.setProperty(AWS_S3_ENDPOINT_PROPERTY, ENDPOINT);
  }

  @Test
  void typicalAmazonClient() {
    AmazonS3 amazonS3 = commonBeans.amazonS3();
    URL url = amazonS3.getUrl(BUCKET, KEY);
    assertThat(url.getHost()).isEqualTo(String.join(".", BUCKET, AWS_ENDPOINT));
  }

  @Test
  void endpointConfiguredAmazonClient() {
    System.setProperty(AWS_S3_ENDPOINT_PROPERTY, ENDPOINT);
    AmazonS3 amazonS3 = commonBeans.amazonS3Test();
    URL url = amazonS3.getUrl(BUCKET, KEY);
    assertThat(url.getHost()).isEqualTo(String.join(".", BUCKET, ENDPOINT));
  }

  @Test
  void s3Client() {
    AmazonS3 amazonS3 = commonBeans.amazonS3();
    S3Client s3Client = new S3Client(amazonS3, false);
    S3Client beansS3Client = commonBeans.s3Client(amazonS3, false);
    assertThat(s3Client).isEqualToComparingFieldByField(beansS3Client);
  }

  @Test
  void verifyS3pathCleaner() {
    S3Client s3Client = commonBeans.s3Client(commonBeans.amazonS3(), false);
    MeterRegistry meterRegistry = mock(GraphiteMeterRegistry.class);
    PathCleaner pathCleaner = commonBeans.s3PathCleaner(s3Client, new BytesDeletedReporter(meterRegistry, false));
    assertThat(pathCleaner).isInstanceOf(S3PathCleaner.class);
  }

  @Test
  void cleanupService() {
    CleanupService cleanupService = commonBeans.cleanupService(Collections.emptyList(), 2, false);
    assertThat(cleanupService).isInstanceOf(PagingCleanupService.class);
  }
}
