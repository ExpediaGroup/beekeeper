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

import java.util.TimeZone;

import com.expediagroup.beekeeper.scheduler.apiary.app.PathSchedulerApiaryRunner;
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
public class BeekeeperPathSchedulerApiary implements ApplicationContextAware {

  private static MeterRegistry meterRegistry;
  private static ConfigurableApplicationContext context;
  private static PathSchedulerApiaryRunner runner;

  public static void main(String[] args) {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    new SpringApplicationBuilder(BeekeeperPathSchedulerApiary.class)
        .properties("spring.config.additional-location:classpath:/beekeeper-path-scheduler-apiary-application.yml,"
            + "${config:null}")
        .build()
        .run(args);
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    context = (ConfigurableApplicationContext) applicationContext;
    runner = (PathSchedulerApiaryRunner) context.getBean("pathSchedulerApiaryRunner");
    meterRegistry = (MeterRegistry) context.getBean("meterRegistry");
  }

  @VisibleForTesting
  public static boolean isRunning() {
    return context != null && context.isRunning();
  }

  @VisibleForTesting
  public static void stop() {
    if (runner == null) {
      throw new RuntimeException("Application runner has not been started.");
    }
    if (context == null) {
      throw new RuntimeException("Application context has not been started.");
    }
    runner.destroy();
    context.close();
  }

  @VisibleForTesting
  public static MeterRegistry meterRegistry() {
    return meterRegistry;
  }
}
