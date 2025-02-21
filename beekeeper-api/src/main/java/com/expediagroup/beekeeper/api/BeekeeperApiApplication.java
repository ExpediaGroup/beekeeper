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
package com.expediagroup.beekeeper.api;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EnableConfigurationProperties
@EntityScan(basePackages = { "com.expediagroup.beekeeper.core.model" })
@EnableJpaRepositories(basePackages = { "com.expediagroup.beekeeper.core.repository" })
@ComponentScan(basePackages = {
    "com.expediagroup.beekeeper.api.conf",
    "com.expediagroup.beekeeper.api.controller",
    "com.expediagroup.beekeeper.api.service",
    "com.expediagroup.beekeeper.api.error" })
public class BeekeeperApiApplication {
  public static void main(String[] args) {
    new SpringApplicationBuilder(BeekeeperApiApplication.class)
        .properties("server.port:${endpoint.port:7008}")
        .build()
        .run(args);
  }
}
