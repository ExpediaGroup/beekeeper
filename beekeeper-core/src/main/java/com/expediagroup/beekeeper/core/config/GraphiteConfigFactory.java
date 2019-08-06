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

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import io.micrometer.graphite.GraphiteConfig;
import io.micrometer.graphite.GraphiteProtocol;

import com.expediagroup.beekeeper.core.error.BeekeeperException;

@Component
@ConfigurationProperties(prefix = "graphite")
public class GraphiteConfigFactory {

  private static int DEFAULT_GRAPHITE_PORT = 2003;
  private static String DEFAULT_GRAPHITE_HOST = "localhost";

  private boolean enabled;
  private String host;
  private String prefix;
  private int port = DEFAULT_GRAPHITE_PORT;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public String getPrefix() {
    return prefix;
  }

  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public GraphiteConfig newInstance() {
    if (!enabled) {
      return new DefaultGraphiteConfig();
    }
    if (host == null) {
      throw new BeekeeperException("If Graphite is enabled, graphite.host must be set.");
    }
    if (prefix == null) {
      throw new BeekeeperException("If Graphite is enabled, graphite.prefix must be set.");
    }
    return new DefaultGraphiteConfig(true, host, prefix, port);
  }

  private static class DefaultGraphiteConfig implements GraphiteConfig {

    private boolean enabled;
    private String host;
    private String prefix;
    private int port;

    public DefaultGraphiteConfig() {
      this(false, DEFAULT_GRAPHITE_HOST, null, DEFAULT_GRAPHITE_PORT);
    }

    public DefaultGraphiteConfig(boolean enabled, String host, String prefix, int port) {
      this.enabled = enabled;
      this.host = host;
      this.prefix = prefix;
      this.port = port;
    }

    @Override
    public String host() {
      return host;
    }

    @Override
    public int port() {
      return port;
    }

    @Override
    public String prefix() {
      return prefix;
    }

    @Override
    public boolean enabled() {
      return enabled;
    }

    @Override
    public GraphiteProtocol protocol() {
      return GraphiteProtocol.PLAINTEXT;
    }

    @Override
    public String get(String key) {
      return null;
    }
  }
}
