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
package com.expediagroup.beekeeper.integration.api;

import static java.lang.String.format;

import java.sql.SQLException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.util.SocketUtils;

import com.expediagroup.beekeeper.api.BeekeeperApiApplication;
import com.expediagroup.beekeeper.api.service.HousekeepingMetadataService;
import com.expediagroup.beekeeper.integration.BeekeeperIntegrationTestBase;

public class BeekeeperApiIntegrationTest extends BeekeeperIntegrationTestBase {

  private static final Logger log = LoggerFactory.getLogger(BeekeeperApiIntegrationTest.class);

  // APP CONTEXT AND TEST CLIENT
  protected static ConfigurableApplicationContext context;
  protected HousekeepingMetadataService testClient;

  @BeforeEach
  public void beforeEach() {
    int port = SocketUtils.findAvailableTcpPort();
    String[] args = new String[] {
        "--server.port=" + port};
    final String url = format("http://localhost:%d", port);
    log.info("Starting to run Beekeeper API on: {} and args: {}", url, args);
    context = SpringApplication.run(BeekeeperApiApplication.class, args);
  }
  
  @AfterEach
  public final void afterEach() {
    log.info("Stopping Beekeeper API");
    if (context != null) {
      context.close();
      context = null;
    }
  }

  @Test
  public void test() throws SQLException, InterruptedException {
    insertExpiredMetadata("s3://path/to/s3/table", "partition=random/partition");
    Thread.sleep(1000000L);
  }

}
