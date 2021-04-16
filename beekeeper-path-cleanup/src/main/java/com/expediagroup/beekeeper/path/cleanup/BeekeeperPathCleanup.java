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
package com.expediagroup.beekeeper.path.cleanup;

import java.util.TimeZone;

import org.springframework.beans.BeansException;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;

import io.micrometer.core.instrument.MeterRegistry;

import com.google.common.annotations.VisibleForTesting;

@SpringBootApplication
@EnableConfigurationProperties
public class BeekeeperPathCleanup implements ApplicationContextAware {

  private static MeterRegistry meterRegistry;
  private static ConfigurableApplicationContext context;

  public static void main(String[] args) {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    new SpringApplicationBuilder(BeekeeperPathCleanup.class)
        .properties(
            "spring.config.additional-location:classpath:/beekeeper-path-cleanup-application.yml,${config:/}")
        .build()
        .run(args);
  }

  @VisibleForTesting
  public static boolean isRunning() {
    return context != null && context.isRunning();
  }

  @VisibleForTesting
  public static void stop() {
    if (context == null) {
      throw new RuntimeException("Application context has not been started.");
    }
    context.close();
  }

  @VisibleForTesting
  public static MeterRegistry meterRegistry() {
    return meterRegistry;
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    context = (ConfigurableApplicationContext) applicationContext;
    meterRegistry = (MeterRegistry) context.getBean("compositeMeterRegistry");
  }
}
