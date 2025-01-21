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

import java.util.EnumMap;
import java.util.List;
import java.util.function.Supplier;

import org.apache.hadoop.hive.conf.HiveConf;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.retry.annotation.EnableRetry;

import com.expedia.apiary.extensions.receiver.common.event.AddPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.CreateTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageReader;
import com.expedia.apiary.extensions.receiver.sqs.messaging.SqsMessageReader;

import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.repository.BeekeeperHistoryRepository;
import com.expediagroup.beekeeper.core.service.BeekeeperHistoryService;
import com.expediagroup.beekeeper.scheduler.apiary.filter.EventTypeListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.IcebergTableListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.ListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.LocationOnlyUpdateListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.TableParameterListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.WhitelistedListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.generator.ExpiredHousekeepingMetadataGenerator;
import com.expediagroup.beekeeper.scheduler.apiary.generator.HousekeepingEntityGenerator;
import com.expediagroup.beekeeper.scheduler.apiary.generator.UnreferencedHousekeepingPathGenerator;
import com.expediagroup.beekeeper.scheduler.apiary.handler.MessageEventHandler;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.BeekeeperEventReader;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.MessageReaderAdapter;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.RetryingMessageReader;
import com.expediagroup.beekeeper.scheduler.hive.HiveClientFactory;
import com.expediagroup.beekeeper.scheduler.hive.PartitionIteratorFactory;
import com.expediagroup.beekeeper.scheduler.service.SchedulerService;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.closeable.CloseableMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.supplier.HiveMetaStoreClientSupplier;

@Configuration
@ComponentScan(basePackages = { "com.expediagroup.beekeeper.core", "com.expediagroup.beekeeper.scheduler" })
@EntityScan(basePackages = { "com.expediagroup.beekeeper.core" })
@EnableJpaRepositories(basePackages = { "com.expediagroup.beekeeper.core.repository" })
@EnableRetry(proxyTargetClass = true)
public class CommonBeans {

  @Bean
  public EnumMap<LifecycleEventType, SchedulerService> schedulerServiceMap(List<SchedulerService> schedulerServices) {
    EnumMap<LifecycleEventType, SchedulerService> schedulerMap = new EnumMap<>(LifecycleEventType.class);
    schedulerServices.forEach(scheduler -> schedulerMap.put(scheduler.getLifecycleEventType(), scheduler));
    return schedulerMap;
  }

  @Bean(name = "sqsMessageReader")
  public MessageReader messageReader(@Value("${properties.apiary.queue-url}") String queueUrl) {
    return new SqsMessageReader.Builder(queueUrl).build();
  }

  @Bean(name = "retryingMessageReader")
  public MessageReader retryingMessageReader(@Qualifier("sqsMessageReader") MessageReader messageReader) {
    return new RetryingMessageReader(messageReader);
  }

  @Bean(name = "unreferencedHousekeepingPathGenerator")
  public HousekeepingEntityGenerator unreferencedHousekeepingPathGenerator(
      @Value("${properties.beekeeper.default-cleanup-delay}") String cleanupDelay) {
    return new UnreferencedHousekeepingPathGenerator(cleanupDelay);
  }

  @Bean(name = "unreferencedHousekeepingPathMessageEventHandler")
  public MessageEventHandler unreferencedHousekeepingPathMessageEventHandler(
      @Qualifier("unreferencedHousekeepingPathGenerator") HousekeepingEntityGenerator generator) {
    List<Class<? extends ListenerEvent>> eventClasses = List.of(
        AlterPartitionEvent.class,
        AlterTableEvent.class,
        DropPartitionEvent.class,
        DropTableEvent.class
    );

    List<ListenerEventFilter> filters = List.of(
        new EventTypeListenerEventFilter(eventClasses),
        new LocationOnlyUpdateListenerEventFilter(),
        new TableParameterListenerEventFilter(),
        new WhitelistedListenerEventFilter(),
        new IcebergTableListenerEventFilter()
    );

    return new MessageEventHandler(generator, filters);
  }

  @Bean(name = "expiredHousekeepingMetadataGenerator")
  public HousekeepingEntityGenerator expiredHousekeepingMetadataGenerator(
      @Value("${properties.beekeeper.default-expiration-delay}") String cleanupDelay) {
    return new ExpiredHousekeepingMetadataGenerator(cleanupDelay);
  }

  @Bean(name = "expiredHousekeepingMetadataMessageEventHandler")
  public MessageEventHandler expiredHousekeepingMetadataMessageEventHandler(
      @Qualifier("expiredHousekeepingMetadataGenerator") HousekeepingEntityGenerator generator) {
    List<Class<? extends ListenerEvent>> eventClasses = List.of(
        CreateTableEvent.class,
        AlterTableEvent.class,
        AddPartitionEvent.class,
        AlterPartitionEvent.class
    );

    List<ListenerEventFilter> filters = List.of(
        new EventTypeListenerEventFilter(eventClasses),
        new TableParameterListenerEventFilter(),
        new IcebergTableListenerEventFilter()
    );

    return new MessageEventHandler(generator, filters);
  }

  @Bean
  public BeekeeperEventReader eventReader(
      @Qualifier("retryingMessageReader") MessageReader messageReader,
      @Qualifier("unreferencedHousekeepingPathMessageEventHandler") MessageEventHandler unreferencedHousekeepingPathMessageEventHandler,
      @Qualifier("expiredHousekeepingMetadataMessageEventHandler") MessageEventHandler expiredHousekeepingMetadataMessageEventHandler
  ) {
    List<MessageEventHandler> handlers = List.of(
        unreferencedHousekeepingPathMessageEventHandler,
        expiredHousekeepingMetadataMessageEventHandler
    );

    return new MessageReaderAdapter(messageReader, handlers);
  }

  @Bean
  BeekeeperHistoryService beekeeperHistoryService(BeekeeperHistoryRepository beekeeperHistoryRepository) {
    return new BeekeeperHistoryService(beekeeperHistoryRepository);
  }

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
  Supplier<CloseableMetaStoreClient> metaStoreClientSupplier(CloseableMetaStoreClientFactory metaStoreClientFactory,
      HiveConf hiveConf) {
    String name = "beekeeper-scheduler";
    return new HiveMetaStoreClientSupplier(metaStoreClientFactory, hiveConf, name);
  }

  @Bean
  public PartitionIteratorFactory partitionIteratorFactory() {
    return new PartitionIteratorFactory();
  }

  @Bean(name = "hiveClientFactory")
  public HiveClientFactory clientFactory(Supplier<CloseableMetaStoreClient> metaStoreClientSupplier,
      PartitionIteratorFactory partitionIteratorFactory) {
    return new HiveClientFactory(metaStoreClientSupplier, partitionIteratorFactory);
  }
}
