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
package com.expediagroup.beekeeper.core.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import io.micrometer.prometheus.PrometheusConfig;

@Component
@ConfigurationProperties(prefix = "prometheus")
public class PrometheusConfigFactory {

  private static String DEFAULT_PREFIX = "beekeeper";

  private String prefix;

  public String getPrefix() {
    return prefix;
  }

  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  public PrometheusConfig newInstance() {
    if (prefix == null) {
      return new PrometheusConfigFactory.DefaultPrometheusConfig(DEFAULT_PREFIX);
    }
    return new PrometheusConfigFactory.DefaultPrometheusConfig(prefix);
  }

  private static class DefaultPrometheusConfig implements PrometheusConfig {

    private String prefix;

    public DefaultPrometheusConfig(String prefix) {
      this.prefix = prefix;
    }

    @Override
    public String prefix() {
      return prefix;
    }

    @Override
    public String get(String key) {
      return null;
    }
  }
}
