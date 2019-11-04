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
package com.expediagroup.beekeeper.core.context;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.micrometer.core.aop.TimedAspect;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;
import io.micrometer.graphite.GraphiteConfig;
import io.micrometer.graphite.GraphiteMeterRegistry;

import com.expediagroup.beekeeper.core.config.GraphiteConfigFactory;

@Configuration
public class MonitoringContext {

  @Bean
  GraphiteMeterRegistry graphiteMeterRegistry(GraphiteConfigFactory graphiteConfigFactory) {
    GraphiteConfig graphiteConfig = graphiteConfigFactory.newInstance();
    GraphiteMeterRegistry graphiteMeterRegistry = new GraphiteMeterRegistry(graphiteConfig, Clock.SYSTEM,
        (id, convention) -> graphiteConfig.prefix()
            + "."
            + HierarchicalNameMapper.DEFAULT.toHierarchicalName(id, convention));
    graphiteMeterRegistry.config()
        .namingConvention(NamingConvention.dot);
    return graphiteMeterRegistry;
  }

  @Bean
  public TimedAspect timedAspect(MeterRegistry registry) {
    return new TimedAspect(registry);
  }
}
