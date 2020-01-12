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
package com.expediagroup.beekeeper.scheduler.apiary.context;

import java.util.EnumMap;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.retry.annotation.EnableRetry;

import com.expedia.apiary.extensions.receiver.common.messaging.MessageReader;
import com.expedia.apiary.extensions.receiver.sqs.messaging.SqsMessageReader;

import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.scheduler.apiary.filter.FilterType;
import com.expediagroup.beekeeper.scheduler.apiary.filter.ListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.handler.MessageEventHandler;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.BeekeeperEventReader;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.MessageReaderAdapter;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.RetryingMessageReader;
import com.expediagroup.beekeeper.scheduler.service.SchedulerService;

@Configuration
@ComponentScan(basePackages = { "com.expediagroup.beekeeper.core", "com.expediagroup.beekeeper.scheduler" })
@EntityScan(basePackages = { "com.expediagroup.beekeeper.core" })
@EnableJpaRepositories(basePackages = { "com.expediagroup.beekeeper.core.repository" })
@EnableRetry(proxyTargetClass = true)
public class CommonBeans {

  @Autowired private List<SchedulerService> schedulerServices;
  @Autowired private List<MessageEventHandler> handlers;
  @Autowired private List<ListenerEventFilter> filters;

  @Bean
  public EnumMap<LifecycleEventType, SchedulerService> schedulerServiceMap(List<SchedulerService> schedulerServices) {
    EnumMap<LifecycleEventType, SchedulerService> schedulerMap = new EnumMap<>(LifecycleEventType.class);
    schedulerServices.stream().forEach(scheduler -> schedulerMap.put(scheduler.getLifecycleEventType(), scheduler));
    return schedulerMap;
  }

  @Bean
  public EnumMap<FilterType, ListenerEventFilter> filterTypeMap(List<ListenerEventFilter> filters) {
    EnumMap<FilterType, ListenerEventFilter> filterTypeMap = new EnumMap<>(FilterType.class);
    filters.stream().forEach(filter -> filterTypeMap.put(filter.getFilterType(), filter));
    return filterTypeMap;
  }

  @Bean(name = "sqsMessageReader")
  public MessageReader messageReader(
      @Value("${properties.apiary.queue-url}") String queueUrl
  ) {
    return new SqsMessageReader.Builder(queueUrl).build();
  }

  @Bean(name = "retryingMessageReader")
  public MessageReader retryingMessageReader(@Qualifier("sqsMessageReader") MessageReader messageReader) {
    return new RetryingMessageReader(messageReader);
  }

  @Bean
  public BeekeeperEventReader pathEventReader(
      @Qualifier("retryingMessageReader") MessageReader messageReader,
      List<MessageEventHandler> handlers
  ) {
    return new MessageReaderAdapter(messageReader, handlers);
  }
}
