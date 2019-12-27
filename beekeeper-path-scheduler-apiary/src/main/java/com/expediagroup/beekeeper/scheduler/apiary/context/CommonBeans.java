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

import com.expediagroup.beekeeper.scheduler.apiary.messaging.FilteringMessageReader;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.MessageEventToPathEventMapper;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.MessageReaderAdapter;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.PathEventReader;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.RetryingMessageReader;

@Configuration
@ComponentScan(basePackages = { "com.expediagroup.beekeeper.core", "com.expediagroup.beekeeper.scheduler" })
@EntityScan(basePackages = { "com.expediagroup.beekeeper.core" })
@EnableJpaRepositories(basePackages = { "com.expediagroup.beekeeper.core.repository" })
@EnableRetry(proxyTargetClass = true)
public class CommonBeans {

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
  MessageReader filteringMessageReader(
    @Qualifier("retryingMessageReader") MessageReader messageReader,
    TableParameterListenerEventFilter tableParameterFilter,
    EventTypeTableListenerEventFilter eventTypeFilter,
    MetadataOnlyListenerEventFilter metadataOnlyListenerEventFilter,
    WhitelistedListenerEventFilter whitelistedListenerEventFilter
  ) {
    List<ListenerEventFilter> filters = List.of(
      eventTypeFilter,
      tableParameterFilter,
      metadataOnlyListenerEventFilter,
      whitelistedListenerEventFilter
    );

    return new FilteringMessageReader(messageReader, filters);
  }

  @Bean
  PathEventReader pathEventReader(@Qualifier("filteringMessageReader") MessageReader messageReader,
    MessageEventToPathEventMapper mapper) {
    return new MessageReaderAdapter(messageReader, mapper);
  }
}
