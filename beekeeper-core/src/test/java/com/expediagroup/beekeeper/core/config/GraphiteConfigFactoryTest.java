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
package com.expediagroup.beekeeper.core.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.micrometer.graphite.GraphiteConfig;

import com.expediagroup.beekeeper.core.error.BeekeeperException;

public class GraphiteConfigFactoryTest {

  private static int DEFAULT_PORT = 2003;
  private static int PORT = 1000;

  private GraphiteConfigFactory graphiteConfigFactory = new GraphiteConfigFactory();

  @Test
  public void typicalEnabledGraphiteConfig() {
    graphiteConfigFactory.setEnabled(true);
    graphiteConfigFactory.setHost("host");
    graphiteConfigFactory.setPrefix("prefix");
    graphiteConfigFactory.setPort(PORT);
    GraphiteConfig graphiteConfig = graphiteConfigFactory.newInstance();
    assertThat(graphiteConfig.enabled()).isTrue();
    assertThat(graphiteConfig.port()).isEqualTo(PORT);
  }

  @Test
  public void typicalEnabledGraphiteConfigDefaultPort() {
    graphiteConfigFactory.setEnabled(true);
    graphiteConfigFactory.setHost("host");
    graphiteConfigFactory.setPrefix("prefix");
    GraphiteConfig graphiteConfig = graphiteConfigFactory.newInstance();
    assertThat(graphiteConfig.enabled()).isTrue();
    assertThat(graphiteConfig.port()).isEqualTo(DEFAULT_PORT);
  }

  @ParameterizedTest
  @CsvSource({ "false, host, prefix",
               "false, null, prefix",
               "false, host, null",
               "false, null, null" })
  public void disabledGraphiteConfigParameterised(boolean enabled, String host, String prefix) {
    graphiteConfigFactory.setEnabled(enabled);
    graphiteConfigFactory.setHost(host.equals("null") ? null : host);
    graphiteConfigFactory.setPrefix(prefix.equals("null") ? null : prefix);
    GraphiteConfig graphiteConfig = graphiteConfigFactory.newInstance();
    assertThat(graphiteConfig.enabled()).isFalse();
  }

  @ParameterizedTest
  @CsvSource({ "true, null, prefix",
               "true, host, null",
               "true, null, null" })
  public void exceptionEnabledGraphiteConfigParameterised(boolean enabled, String host, String prefix) {
    graphiteConfigFactory.setEnabled(enabled);
    graphiteConfigFactory.setHost(host.equals("null") ? null : host);
    graphiteConfigFactory.setPrefix(prefix.equals("null") ? null : prefix);

    Throwable throwable = catchThrowable(() -> graphiteConfigFactory.newInstance());
    assertThat(throwable).isInstanceOf(BeekeeperException.class);
  }
}
