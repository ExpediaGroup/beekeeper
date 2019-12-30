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
package com.expediagroup.beekeeper.scheduler.apiary;

import com.expediagroup.beekeeper.scheduler.apiary.messaging.RetryingMessageReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;

import com.expedia.apiary.extensions.receiver.common.messaging.MessageReader;
import com.expedia.apiary.extensions.receiver.sqs.messaging.SqsMessageReader;

@Configuration
@EnableRetry(proxyTargetClass=true)
public class TestConfig {

  private static final String URL = "url";

  @Bean
  public SqsMessageReader sqsMessageReader() {
    return new SqsMessageReader.Builder(URL).build();
  }

  @Bean
  public RetryingMessageReader retryingMessageReader(MessageReader messageReader) {
    return new RetryingMessageReader(messageReader);
  }
}
