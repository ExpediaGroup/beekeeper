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

import java.util.List;

import com.expediagroup.beekeeper.scheduler.apiary.filter.*;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.BeekeeperEventReader;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.FilteringMessageReader;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.MessageReaderAdapter;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.RetryingMessageReader;
import com.expediagroup.beekeeper.scheduler.apiary.mapper.MessageEventMapper;
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

@Configuration
@ComponentScan(basePackages = { "com.expediagroup.beekeeper.core", "com.expediagroup.beekeeper.scheduler" })
@EntityScan(basePackages = { "com.expediagroup.beekeeper.core" })
@EnableJpaRepositories(basePackages = { "com.expediagroup.beekeeper.core.repository" })
@EnableRetry(proxyTargetClass = true)
public class CommonBeans {

  @Autowired List<MessageEventMapper> eventMappers;
  @Autowired List<ListenerEventFilter> eventFilters;

  @Value("${properties.apiary.queue-url}")
  private String queueUrl;

  @Bean(name = "sqsMessageReader")
  MessageReader messageReader() {
    return new SqsMessageReader.Builder(queueUrl).build();
  }

  @Bean(name = "retryingMessageReader")
  MessageReader retryingMessageReader(@Qualifier("sqsMessageReader") MessageReader messageReader) {
    return new RetryingMessageReader(messageReader);
  }

  @Bean(name = "filteringMessageReader")
  MessageReader filteringMessageReader(@Qualifier("retryingMessageReader") MessageReader messageReader) {
    return new FilteringMessageReader(messageReader, eventFilters);
  }

  @Bean
  BeekeeperEventReader pathEventReader(@Qualifier("filteringMessageReader") MessageReader messageReader) {
    return new MessageReaderAdapter(messageReader, eventMappers);
  }
}
