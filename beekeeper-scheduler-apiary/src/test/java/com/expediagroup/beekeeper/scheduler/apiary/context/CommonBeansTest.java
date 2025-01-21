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
package com.expediagroup.beekeeper.scheduler.apiary.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.function.Supplier;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.messaging.MessageReader;
import com.expedia.apiary.extensions.receiver.sqs.messaging.SqsMessageReader;

import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.repository.BeekeeperHistoryRepository;
import com.expediagroup.beekeeper.core.service.BeekeeperHistoryService;
import com.expediagroup.beekeeper.scheduler.apiary.filter.IcebergTableListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.ListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.generator.ExpiredHousekeepingMetadataGenerator;
import com.expediagroup.beekeeper.scheduler.apiary.generator.HousekeepingEntityGenerator;
import com.expediagroup.beekeeper.scheduler.apiary.generator.UnreferencedHousekeepingPathGenerator;
import com.expediagroup.beekeeper.scheduler.apiary.handler.MessageEventHandler;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.BeekeeperEventReader;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.RetryingMessageReader;
import com.expediagroup.beekeeper.scheduler.service.SchedulerService;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.closeable.CloseableMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.supplier.HiveMetaStoreClientSupplier;

@ExtendWith(MockitoExtension.class)
public class CommonBeansTest {

  private static final String AWS_S3_ENDPOINT_PROPERTY = "aws.s3.endpoint";
  private static final String AWS_REGION_PROPERTY = "aws.region";
  private static final String REGION = "us-west-2";
  private static final String ENDPOINT = "endpoint";
  private final CommonBeans commonBeans = new CommonBeans();
  private @Mock MessageReader messageReader;
  private @Mock UnreferencedHousekeepingPathGenerator unreferencedHousekeepingPathGenerator;
  private @Mock ExpiredHousekeepingMetadataGenerator expiredHousekeepingMetadataGenerator;
  private @Mock BeekeeperHistoryRepository beekeeperHistoryRepository;

  @AfterAll
  static void tearDown() {
    System.clearProperty(AWS_REGION_PROPERTY);
    System.clearProperty(AWS_S3_ENDPOINT_PROPERTY);
  }

  @BeforeEach
  void setUp() {
    System.setProperty(AWS_REGION_PROPERTY, REGION);
    System.setProperty(AWS_S3_ENDPOINT_PROPERTY, ENDPOINT);
  }

  @Test
  public void validateSchedulerServiceMap() {
    EnumMap<LifecycleEventType, SchedulerService> scheduleMap = commonBeans.schedulerServiceMap(Collections.EMPTY_LIST);
    assertThat(scheduleMap).isInstanceOf(EnumMap.class);
  }

  @Test
  public void validateMessageReader() {
    MessageReader reader = commonBeans.messageReader("some_path");
    assertThat(reader).isInstanceOf(SqsMessageReader.class);
  }

  @Test
  public void validateRetryingMessageReader() {
    MessageReader reader = commonBeans.retryingMessageReader(messageReader);
    assertThat(reader).isInstanceOf(RetryingMessageReader.class);
  }

  @Test
  public void validateUnreferencedHousekeepingPathGenerator() {
    HousekeepingEntityGenerator generator = commonBeans.unreferencedHousekeepingPathGenerator("P30D");
    assertThat(generator).isInstanceOf(UnreferencedHousekeepingPathGenerator.class);
  }

  @Test
  public void validateUnreferencedHousekeepingPathMessageEventHandler() {
    MessageEventHandler handler = commonBeans.unreferencedHousekeepingPathMessageEventHandler(
        unreferencedHousekeepingPathGenerator);
    assertThat(handler).isInstanceOf(MessageEventHandler.class);
  }

  @Test
  public void validateExpiredHousekeepingMetadataGenerator() {
    HousekeepingEntityGenerator generator = commonBeans.expiredHousekeepingMetadataGenerator("P30D");
    assertThat(generator).isInstanceOf(ExpiredHousekeepingMetadataGenerator.class);
  }

  @Test
  public void validateExpiredHousekeepingMetadataMessageEventHandler() {
    MessageEventHandler handler = commonBeans.expiredHousekeepingMetadataMessageEventHandler(
        expiredHousekeepingMetadataGenerator);
    assertThat(handler).isInstanceOf(MessageEventHandler.class);
  }

  @Test
  public void validatePathEventReader() {
    BeekeeperEventReader reader = commonBeans.eventReader(messageReader, mock(MessageEventHandler.class),
        mock(MessageEventHandler.class));
    assertThat(reader).isInstanceOf(BeekeeperEventReader.class);
  }

  @Test
  public void validateUnreferencedHousekeepingPathMessageEventHandlerIncludesIcebergFilter() {
    MessageEventHandler handler = commonBeans.unreferencedHousekeepingPathMessageEventHandler(
        unreferencedHousekeepingPathGenerator);
    List<ListenerEventFilter> filters = handler.getFilters();
    assertThat(filters).hasAtLeastOneElementOfType(IcebergTableListenerEventFilter.class);
  }

  @Test
  public void validateExpiredHousekeepingMetadataMessageEventHandlerIncludesIcebergFilter() {
    MessageEventHandler handler = commonBeans.expiredHousekeepingMetadataMessageEventHandler(
        expiredHousekeepingMetadataGenerator);
    List<ListenerEventFilter> filters = handler.getFilters();
    assertThat(filters).hasAtLeastOneElementOfType(IcebergTableListenerEventFilter.class);
  }

  @Test
  public void verifyBeekeeperHistoryService() {
    BeekeeperHistoryService beekeeperHistoryService = commonBeans.beekeeperHistoryService(beekeeperHistoryRepository);
    assertThat(beekeeperHistoryService).isInstanceOf(BeekeeperHistoryService.class);
  }

  @Test
  public void verifyMetaStoreClientSupplier() {
    CloseableMetaStoreClientFactory metaStoreClientFactory = commonBeans.metaStoreClientFactory();
    HiveConf hiveConf = Mockito.mock(HiveConf.class);

    Supplier<CloseableMetaStoreClient> metaStoreClientSupplier = commonBeans
        .metaStoreClientSupplier(metaStoreClientFactory, hiveConf);
    assertThat(metaStoreClientSupplier).isInstanceOf(HiveMetaStoreClientSupplier.class);
  }
}
