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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.springframework.http.HttpStatus.OK;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.domain.Page;
import org.springframework.util.SocketUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

import com.expediagroup.beekeeper.api.BeekeeperApiApplication;
import com.expediagroup.beekeeper.api.response.HousekeepingMetadataResponse;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.integration.BeekeeperIntegrationTestBase;
import com.expediagroup.beekeeper.integration.utils.BeekeeperApiTestClient;
import com.expediagroup.beekeeper.integration.utils.RestResponsePage;

public class BeekeeperApiIntegrationTest extends BeekeeperIntegrationTestBase {

  public ObjectMapper geObjMapper() {
    return new ObjectMapper()
        .registerModule(new ParameterNamesModule())
        .registerModule(new JavaTimeModule());
  }

  private static final Logger log = LoggerFactory.getLogger(BeekeeperApiIntegrationTest.class);

  // APP CONTEXT AND TEST CLIENT
  protected static ConfigurableApplicationContext context;
  protected BeekeeperApiTestClient testClient;

  protected final ObjectMapper mapper = geObjMapper();

  @BeforeEach
  public void beforeEach() {
    int port = SocketUtils.findAvailableTcpPort();
    String[] args = new String[] { "--server.port=" + port };
    final String url = format("http://localhost:%d", port);
    log.info("Starting to run Beekeeper API on: {} and args: {}", url, args);
    context = SpringApplication.run(BeekeeperApiApplication.class, args);
    testClient = new BeekeeperApiTestClient(url);
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
  public void testGetMetadataWhenTableNotFound() throws SQLException, InterruptedException, IOException {
    HousekeepingMetadata testMetadata1 = createHousekeepingMetadata("wrong_table",
        "s3://some/path/event_date=2020-01-01/event_hour=0/event_type=A",
        "event_date=2020-01-01/event_hour=0/event_type=A", LifecycleEventType.EXPIRED,
        Duration.parse("P3D").toString());
    HousekeepingMetadata testMetadata2 = createHousekeepingMetadata("wrong_table",
        "s3://some/path/event_date=2020-01-01/event_hour=0/event_type=B",
        "event_date=2020-01-01/event_hour=0/event_type=B", LifecycleEventType.EXPIRED,
        Duration.parse("P3D").toString());
    insertExpiredMetadata(testMetadata1);
    insertExpiredMetadata(testMetadata2);

    HttpResponse<String> response = testClient.getMetadata();
    assertThat(response.statusCode()).isEqualTo(OK.value());
    String body = response.body();
    assertThrows(ValueInstantiationException.class,
        () -> mapper.readValue(body, new TypeReference<RestResponsePage<HousekeepingMetadataResponse>>() {}));
  }

  @Test
  public void testGetMetadataWhenThereIsFiltering() throws SQLException, InterruptedException, IOException {
    HousekeepingMetadata testMetadata1 = createHousekeepingMetadata("some_table",
        "s3://some/path/event_date=2020-01-01/event_hour=0/event_type=A",
        "event_date=2020-01-01/event_hour=0/event_type=A", LifecycleEventType.EXPIRED,
        Duration.parse("P3D").toString());
    HousekeepingMetadata testMetadata2 = createHousekeepingMetadata("some_table",
        "s3://some/path/event_date=2020-01-01/event_hour=0/event_type=B",
        "event_date=2020-01-01/event_hour=0/event_type=B", LifecycleEventType.UNREFERENCED,
        Duration.parse("P3D").toString());
    HousekeepingMetadata testMetadata3 = createHousekeepingMetadata("some_table",
        "s3://some/path/event_date=2020-01-01/event_hour=0/event_type=C",
        "event_date=2020-01-01/event_hour=0/event_type=C", LifecycleEventType.UNREFERENCED,
        Duration.parse("P3D").toString());
    testMetadata1.setHousekeepingStatus(HousekeepingStatus.FAILED);
    testMetadata1.setCleanupTimestamp(LocalDateTime.parse("1999-05-05T10:41:20"));
    testMetadata1.setCreationTimestamp(LocalDateTime.parse("1999-05-05T10:41:20"));
    insertExpiredMetadata(testMetadata1);
    insertExpiredMetadata(testMetadata2);
    insertExpiredMetadata(testMetadata3);

    String filters = "?housekeeping_status=FAILED"
        + "&lifecycle_type=EXPIRED"
        + "&deleted_before=2000-05-05T10:41:20"
        + "&registered_before=2000-04-04T10:41:20"
        + "&path_name=s3://some/path/event_date=2020-01-01/event_hour=0/event_type=A"
        + "&partition_name=event_date=2020-01-01/event_hour=0/event_type=A";

    HttpResponse<String> response = testClient.getMetadata(filters);
    assertThat(response.statusCode()).isEqualTo(OK.value());
    String body = response.body();
    Page<HousekeepingMetadataResponse> responsePage = mapper
        .readValue(body, new TypeReference<RestResponsePage<HousekeepingMetadataResponse>>() {});
    List<HousekeepingMetadataResponse> result = responsePage.getContent();

    assertThatMetadataEqualsResponse(testMetadata1,result.get(0));
    assertThat(result.size()).isEqualTo(1);
  }

  // This test is to manually test the API
  @Disabled
  @Test
  public void manualTest() throws SQLException, InterruptedException {
    HousekeepingMetadata testMetadata1 = createHousekeepingMetadata("some_table",
        "s3://some/path/event_date=2020-01-01/event_hour=0/event_type=A",
        "event_date=2020-01-01/event_hour=0/event_type=A", LifecycleEventType.EXPIRED,
        Duration.parse("P3D").toString());
    HousekeepingMetadata testMetadata2 = createHousekeepingMetadata("some_table",
        "s3://some/path/event_date=2020-01-01/event_hour=0/event_type=B",
        "event_date=2020-01-01/event_hour=0/event_type=B", LifecycleEventType.EXPIRED,
        Duration.parse("P3D").toString());
    HousekeepingMetadata testMetadata3 = createHousekeepingMetadata("some_table",
        "s3://some/path/event_date=2020-01-01/event_hour=0/event_type=C",
        "event_date=2020-01-01/event_hour=0/event_type=C", LifecycleEventType.UNREFERENCED,
        Duration.parse("P4D").toString());
    insertExpiredMetadata(testMetadata1);
    insertExpiredMetadata(testMetadata2);
    insertExpiredMetadata(testMetadata3);

    Thread.sleep(10000000L);
  }
}
