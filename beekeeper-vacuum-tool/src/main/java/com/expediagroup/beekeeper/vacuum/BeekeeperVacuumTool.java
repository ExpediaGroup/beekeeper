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
package com.expediagroup.beekeeper.vacuum;

import java.util.TimeZone;

import org.springframework.beans.BeansException;
import org.springframework.boot.Banner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class BeekeeperVacuumTool implements ApplicationContextAware {

  private static ConfigurableApplicationContext context;

  public static void main(String[] args) {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

    new SpringApplicationBuilder(BeekeeperVacuumTool.class)
        .properties("spring.jpa.hibernate.ddl-auto=validate")
        .properties("spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.MySQL8Dialect")
        .properties("spring.jpa.database=default")
        .registerShutdownHook(true)
        .bannerMode(Banner.Mode.OFF)
        .build()
        .run(args);
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    context = (ConfigurableApplicationContext) applicationContext;
  }
}
