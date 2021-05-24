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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.http.HttpStatus.OK;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.util.SocketUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
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

  @Bean
  @Primary
  public ObjectMapper geObjMapper() {
    return new ObjectMapper()
        .registerModule(new ParameterNamesModule())
        .registerModule(new Jdk8Module())
        .registerModule(new JavaTimeModule());
  }

  private static final Logger log = LoggerFactory.getLogger(BeekeeperApiIntegrationTest.class);

  // APP CONTEXT AND TEST CLIENT
  protected static ConfigurableApplicationContext context;
  protected BeekeeperApiTestClient testClient;
  
  protected final ObjectMapper mapper = geObjMapper();

  @BeforeEach
  public void beforeEach() {

    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    int port = SocketUtils.findAvailableTcpPort();
    String[] args = new String[] {
        "--server.port=" + port};
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
  public void testGetTablesWhenNoTables() throws InterruptedException, IOException {
    HttpResponse<String> response = testClient.getTables();
    assertThat(response.statusCode()).isEqualTo(OK.value());
    String body = response.body();
    Page<HousekeepingMetadata> responsePage = mapper
        .readValue(body, new TypeReference<RestResponsePage<HousekeepingMetadata>>() {});
    assertThat(responsePage.getContent()).isEqualTo(List.of());
  }
  
  @Test
  public void testGetTablesWhenTablesValid() throws SQLException, InterruptedException, IOException {

    HousekeepingMetadata testMetadata1 = createHousekeepingMetadata("myTableName1","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    HousekeepingMetadata testMetadata2 = createHousekeepingMetadata("myTableName2","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    insertExpiredMetadata(testMetadata1);
    insertExpiredMetadata(testMetadata2);

    HttpResponse<String> response = testClient.getTables();
    assertThat(response.statusCode()).isEqualTo(OK.value());
    String body = response.body();
    Page<HousekeepingMetadata> responsePage = mapper
        .readValue(body, new TypeReference<RestResponsePage<HousekeepingMetadata>>() {});
    List<HousekeepingMetadata> result = responsePage.getContent();

    assertTrue(testMetadata1.equals(result.get(0)));
    assertTrue(testMetadata2.equals(result.get(1)));
    assertThat(result.size()).isEqualTo(2);

  }

  @Test
  public void testGetTablesWhenTableNameFilter() throws SQLException, InterruptedException, IOException {

    HousekeepingMetadata testMetadata1 = createHousekeepingMetadata("myTableName1","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    HousekeepingMetadata testMetadata2 = createHousekeepingMetadata("myTableName2","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    HousekeepingMetadata metadataWithTableName = createHousekeepingMetadata("bobs_table","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    insertExpiredMetadata(testMetadata1);
    insertExpiredMetadata(testMetadata2);
    insertExpiredMetadata(metadataWithTableName);
    
    HttpResponse<String> response = testClient.getTablesWithTableNameFilter("bobs_table");
    assertThat(response.statusCode()).isEqualTo(OK.value());
    String body = response.body();
    Page<HousekeepingMetadata> responsePage = mapper
        .readValue(body, new TypeReference<RestResponsePage<HousekeepingMetadata>>() {});
    List<HousekeepingMetadata> result = responsePage.getContent();

    assertTrue(metadataWithTableName.equals(result.get(0)));
    assertThat(result.size()).isEqualTo(1);
  }
  
  @Test
  public void testGetTablesWhenDatabaseNameFilter() throws SQLException, InterruptedException, IOException {

    HousekeepingMetadata testMetadata1 = createHousekeepingMetadata("myTableName1","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    HousekeepingMetadata testMetadata2 = createHousekeepingMetadata("myTableName2","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    HousekeepingMetadata metadataWithDatabaseName = createHousekeepingMetadata("bobs_table","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    metadataWithDatabaseName.setDatabaseName("bobs_database");
    insertExpiredMetadata(testMetadata1);
    insertExpiredMetadata(testMetadata2);
    insertExpiredMetadata(metadataWithDatabaseName);
    
    HttpResponse<String> response = testClient.getTablesWithDatabaseNameFilter("bobs_database");
    assertThat(response.statusCode()).isEqualTo(OK.value());
    String body = response.body();
    Page<HousekeepingMetadataResponse> responsePage = mapper
        .readValue(body, new TypeReference<RestResponsePage<HousekeepingMetadataResponse>>() {});
    List<HousekeepingMetadataResponse> result = responsePage.getContent();

    assertEquals(metadataWithDatabaseName,result.get(0));
    assertTrue(metadataWithDatabaseName.equals(result.get(0)));
    assertThat(result.size()).isEqualTo(1);
  }
  
  @Test
  public void testGetTablesWhenHousekeepingStatusFilter() throws SQLException, InterruptedException, IOException {

    HousekeepingMetadata testMetadata1 = createHousekeepingMetadata("myTableName1","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    HousekeepingMetadata testMetadata2 = createHousekeepingMetadata("myTableName2","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    HousekeepingMetadata metadataWithHousekeepingStatus = createHousekeepingMetadata("bobs_table","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    metadataWithHousekeepingStatus.setHousekeepingStatus(HousekeepingStatus.FAILED);
    insertExpiredMetadata(testMetadata1);
    insertExpiredMetadata(testMetadata2);
    insertExpiredMetadata(metadataWithHousekeepingStatus);
    
    HttpResponse<String> response = testClient.getTablesWithHousekeepingStatusFilter("FAILED");
    assertThat(response.statusCode()).isEqualTo(OK.value());
    String body = response.body();
    Page<HousekeepingMetadata> responsePage = mapper
        .readValue(body, new TypeReference<RestResponsePage<HousekeepingMetadata>>() {});
    List<HousekeepingMetadata> result = responsePage.getContent();

    assertTrue(metadataWithHousekeepingStatus.equals(result.get(0)));
    assertThat(result.size()).isEqualTo(1);
  }

  @Test
  public void testGetTablesWhenLifecycleEventTypeFilter() throws SQLException, InterruptedException, IOException {
    HousekeepingMetadata testMetadata1 = createHousekeepingMetadata("myTableName2","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    HousekeepingMetadata testMetadata2 = createHousekeepingMetadata("myTableName3","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    HousekeepingMetadata unreferencedMetadata = createHousekeepingMetadata("myTableName","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.UNREFERENCED,Duration.parse("P3D").toString());
    insertExpiredMetadata(testMetadata1);
    insertExpiredMetadata(testMetadata2);
    insertExpiredMetadata(unreferencedMetadata);


    HttpResponse<String> response = testClient.getTablesWithLifecycleEventTypeFilter("UNREFERENCED");
    assertThat(response.statusCode()).isEqualTo(OK.value());
    String body = response.body();
    Page<HousekeepingMetadata> responsePage = mapper
        .readValue(body, new TypeReference<RestResponsePage<HousekeepingMetadata>>() {});
    List<HousekeepingMetadata> result = responsePage.getContent();

    assertTrue(unreferencedMetadata.equals(result.get(0)));
    assertThat(result.size()).isEqualTo(1);
  }
  
  @Test
  public void testGetTablesWhenDeletedBeforeFilter() throws SQLException, InterruptedException, IOException {

    HousekeepingMetadata testMetadata1 = createHousekeepingMetadata("myTableName2","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    HousekeepingMetadata testMetadata2 = createHousekeepingMetadata("myTableName3","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    HousekeepingMetadata metadataWithCleanupTimestamp = createHousekeepingMetadata("myTableName","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.UNREFERENCED,Duration.parse("P3D").toString());
    metadataWithCleanupTimestamp.setCleanupTimestamp(LocalDateTime.parse("1999-05-05T10:41:20"));
    insertExpiredMetadata(testMetadata1);
    insertExpiredMetadata(testMetadata2);
    insertExpiredMetadata(metadataWithCleanupTimestamp);
    
    HttpResponse<String> response = testClient.getTablesWithDeletedBeforeFilter("2000-05-05T10:41:20");
    assertThat(response.statusCode()).isEqualTo(OK.value());
    String body = response.body();
    Page<HousekeepingMetadata> responsePage = mapper
        .readValue(body, new TypeReference<RestResponsePage<HousekeepingMetadata>>() {});
    List<HousekeepingMetadata> result = responsePage.getContent();

    assertTrue(metadataWithCleanupTimestamp.equals(result.get(0)));
    assertThat(result.size()).isEqualTo(1);
  }

  @Test
  public void testGetTablesWhenDeletedAfterFilter() throws SQLException, InterruptedException, IOException {

    HousekeepingMetadata testMetadata1 = createHousekeepingMetadata("myTableName2","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    HousekeepingMetadata testMetadata2 = createHousekeepingMetadata("myTableName3","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    HousekeepingMetadata metadataWithCleanupTimestamp = createHousekeepingMetadata("myTableName","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.UNREFERENCED,Duration.parse("P3D").toString());
    testMetadata1.setCleanupTimestamp(LocalDateTime.parse("2020-05-05T10:41:20"));
    testMetadata2.setCleanupTimestamp(LocalDateTime.parse("2020-05-05T10:41:20"));
    metadataWithCleanupTimestamp.setCleanupTimestamp(LocalDateTime.parse("2020-06-05T10:41:20"));
    insertExpiredMetadata(testMetadata1);
    insertExpiredMetadata(testMetadata2);
    insertExpiredMetadata(metadataWithCleanupTimestamp);

    HttpResponse<String> response = testClient.getTablesWithDeletedAfterFilter("2020-05-06T10:41:20");
    assertThat(response.statusCode()).isEqualTo(OK.value());
    String body = response.body();
    Page<HousekeepingMetadata> responsePage = mapper
        .readValue(body, new TypeReference<RestResponsePage<HousekeepingMetadata>>() {});
    List<HousekeepingMetadata> result = responsePage.getContent();

    assertTrue(metadataWithCleanupTimestamp.equals(result.get(0)));
    assertThat(result.size()).isEqualTo(1);
  }

  @Test
  public void testGetTablesWhenRegisteredBeforeFilter() throws SQLException, InterruptedException, IOException {

    HousekeepingMetadata testMetadata1 = createHousekeepingMetadata("myTableName2","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    HousekeepingMetadata testMetadata2 = createHousekeepingMetadata("myTableName3","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    HousekeepingMetadata metadataWithCreationTimestamp = createHousekeepingMetadata("myTableName","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.UNREFERENCED,Duration.parse("P3D").toString());
    testMetadata1.setCreationTimestamp(LocalDateTime.parse("2020-05-05T10:41:20"));
    testMetadata2.setCreationTimestamp(LocalDateTime.parse("2020-05-05T10:41:20"));
    metadataWithCreationTimestamp.setCreationTimestamp(LocalDateTime.parse("2020-04-05T10:41:20"));
    insertExpiredMetadata(testMetadata1);
    insertExpiredMetadata(testMetadata2);
    insertExpiredMetadata(metadataWithCreationTimestamp);

    HttpResponse<String> response = testClient.getTablesWithRegisteredBeforeFilter("2020-05-04T10:41:20");
    assertThat(response.statusCode()).isEqualTo(OK.value());
    String body = response.body();
    Page<HousekeepingMetadata> responsePage = mapper
        .readValue(body, new TypeReference<RestResponsePage<HousekeepingMetadata>>() {});
    List<HousekeepingMetadata> result = responsePage.getContent();

    assertTrue(metadataWithCreationTimestamp.equals(result.get(0)));
    assertThat(result.size()).isEqualTo(1);
  }

  @Test
  public void testGetTablesWhenRegisteredAfterFilter() throws SQLException, InterruptedException, IOException {

    HousekeepingMetadata testMetadata1 = createHousekeepingMetadata("myTableName2","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    HousekeepingMetadata testMetadata2 = createHousekeepingMetadata("myTableName3","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    HousekeepingMetadata metadataWithCreationTimestamp = createHousekeepingMetadata("myTableName","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.UNREFERENCED,Duration.parse("P3D").toString());
    testMetadata1.setCreationTimestamp(LocalDateTime.parse("2020-05-05T10:41:20"));
    testMetadata2.setCreationTimestamp(LocalDateTime.parse("2020-05-05T10:41:20"));
    metadataWithCreationTimestamp.setCreationTimestamp(LocalDateTime.parse("2020-06-05T10:41:20"));
    insertExpiredMetadata(testMetadata1);
    insertExpiredMetadata(testMetadata2);
    insertExpiredMetadata(metadataWithCreationTimestamp);

    HttpResponse<String> response = testClient.getTablesWithRegisteredAfterFilter("2020-05-06T10:41:20");
    assertThat(response.statusCode()).isEqualTo(OK.value());
    String body = response.body();
    Page<HousekeepingMetadata> responsePage = mapper
        .readValue(body, new TypeReference<RestResponsePage<HousekeepingMetadata>>() {});
    List<HousekeepingMetadata> result = responsePage.getContent();

    assertTrue(metadataWithCreationTimestamp.equals(result.get(0)));
    assertThat(result.size()).isEqualTo(1);
  }

  @Test
  public void TESTING() throws SQLException, InterruptedException, IOException {
    HousekeepingMetadata testMetadata1 = createHousekeepingMetadata("myTableName2","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    HousekeepingMetadata testMetadata2 = createHousekeepingMetadata("myTableName3","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    HousekeepingMetadata testMetadata3 = createHousekeepingMetadata("myTableName3","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.UNREFERENCED,Duration.parse("P3D").toString());
    HousekeepingMetadata unreferencedMetadata = createHousekeepingMetadata("myTableName","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=A",LifecycleEventType.UNREFERENCED,Duration.parse("P3D").toString());
    HousekeepingMetadata unreferencedMetadata2 = createHousekeepingMetadata("myTableName","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=B",LifecycleEventType.EXPIRED,Duration.parse("P3D").toString());
    HousekeepingMetadata unreferencedMetadata3 = createHousekeepingMetadata("myTableName","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=C",LifecycleEventType.UNREFERENCED,Duration.parse("P3D").toString());
    HousekeepingMetadata unreferencedMetadata4 = createHousekeepingMetadata("myTableName","s3://some/path/","event_date=2020-01-01/event_hour=0/event_type=C",LifecycleEventType.UNREFERENCED,Duration.parse("P3D").toString());
    testMetadata1.setCleanupDelay(Duration.parse("P4D"));
    insertExpiredMetadata(testMetadata1);
    insertExpiredMetadata(testMetadata2);
    insertExpiredMetadata(testMetadata3);
    insertExpiredMetadata(unreferencedMetadata);
    insertExpiredMetadata(unreferencedMetadata2);
    insertExpiredMetadata(unreferencedMetadata3);
    insertExpiredMetadata(unreferencedMetadata4);

    Thread.sleep(10000000L);

    HttpResponse<String> response = testClient.getTables();
    assertThat(response.statusCode()).isEqualTo(OK.value());
    String body = response.body();
    Page<HousekeepingMetadata> responsePage = mapper
        .readValue(body, new TypeReference<RestResponsePage<HousekeepingMetadata>>() {});
    List<HousekeepingMetadata> result = responsePage.getContent();

  }

}
