/**
 * Copyright (C) 2019-2023 Expedia, Inc.
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
import static org.springframework.http.HttpStatus.OK;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.CLEANUP_ATTEMPTS_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.CLIENT_ID_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.CREATION_TIMESTAMP_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.DATABASE_NAME_VALUE;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
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
import org.springframework.http.HttpStatus;
import org.springframework.util.SocketUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

import com.expediagroup.beekeeper.api.BeekeeperApiApplication;
import com.expediagroup.beekeeper.api.error.ErrorResponse;
import com.expediagroup.beekeeper.api.response.HousekeepingMetadataResponse;
import com.expediagroup.beekeeper.api.response.HousekeepingPathResponse;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.model.PeriodDuration;
import com.expediagroup.beekeeper.integration.BeekeeperIntegrationTestBase;
import com.expediagroup.beekeeper.integration.utils.BeekeeperApiTestClient;
import com.expediagroup.beekeeper.integration.utils.RestResponsePage;

public class BeekeeperApiIntegrationTest extends BeekeeperIntegrationTestBase {

  public ObjectMapper geObjectMapper() {
    return new ObjectMapper().registerModule(new ParameterNamesModule()).registerModule(new JavaTimeModule());
  }
  private static final Logger log = LoggerFactory.getLogger(BeekeeperApiIntegrationTest.class);
  protected static ConfigurableApplicationContext context;
  protected BeekeeperApiTestClient testClient;
  protected final ObjectMapper mapper = geObjectMapper();
  private Long id = 1L;
  protected static final String someTable = "some_table";
  protected static final String someDatabase = "some_database";
  protected static final String pathA = "s3://some/path/event_date=2020-01-01/event_hour=0/event_type=A";
  protected static final String pathB = "s3://some/path/event_date=2020-01-01/event_hour=0/event_type=B";
  protected static final String pathC = "s3://some/path/event_date=2020-01-01/event_hour=0/event_type=C";
  protected static final Duration duration = Duration.parse("P3D");
  protected static final int pageSize = 1;
  protected static final String partitionA = "event_date=2020-01-01/event_hour=0/event_type=A";
  protected static final String partitionB = "event_date=2020-01-01/event_hour=0/event_type=B";
  protected static final String partitionC = "event_date=2020-01-01/event_hour=0/event_type=C";
  protected final HousekeepingPath testPathA = createHousekeepingPath(someTable, pathA, LifecycleEventType.EXPIRED, duration.toString(), HousekeepingStatus.FAILED);
  protected final HousekeepingPath testPathB = createHousekeepingPath(someTable, pathB, LifecycleEventType.UNREFERENCED, duration.toString(), HousekeepingStatus.FAILED);
  protected final HousekeepingPath testPathC = createHousekeepingPath(someTable, pathC, LifecycleEventType.UNREFERENCED, duration.toString(), HousekeepingStatus.FAILED);
  protected final HousekeepingMetadata testMetadataA = createHousekeepingMetadata(someTable, pathA, partitionA, LifecycleEventType.EXPIRED, duration.toString(),HousekeepingStatus.FAILED);
  protected final HousekeepingMetadata testMetadataB = createHousekeepingMetadata(someTable, pathB, partitionB, LifecycleEventType.EXPIRED, duration.toString(),HousekeepingStatus.FAILED);
  protected final HousekeepingMetadata testMetadataC = createHousekeepingMetadata(someTable, pathC, partitionC, LifecycleEventType.EXPIRED, duration.toString(),HousekeepingStatus.FAILED);
  protected final HousekeepingMetadata testMetadataD = createHousekeepingMetadata(someTable, pathC, partitionB, LifecycleEventType.UNREFERENCED, duration.toString(), HousekeepingStatus.SCHEDULED);
  protected final HousekeepingMetadata testMetadataE = createHousekeepingMetadata(someTable, pathC, partitionC, LifecycleEventType.UNREFERENCED, duration.toString(),HousekeepingStatus.SCHEDULED);
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
  public void testGetMetadataWhenTableNotFoundReturnsEmptyList()
      throws SQLException, InterruptedException, IOException {

    for (HousekeepingMetadata testMetadata : Arrays.asList(testMetadataB, testMetadataC)) {
      insertExpiredMetadata(testMetadata);
    }

    HttpResponse<String> response = testClient.getMetadata("wrong_database", "wrong_table");
    assertThat(response.statusCode()).isEqualTo(OK.value());
    String body = response.body();
    Page<HousekeepingMetadataResponse> responsePage = mapper
        .readValue(body, new TypeReference<RestResponsePage<HousekeepingMetadataResponse>>() {});
    assertThat(responsePage.getTotalElements()).isEqualTo(0);
  }

  @Test
  public void testGetMetadataWhenThereIsFiltering() throws SQLException, InterruptedException, IOException {
    testMetadataA.setCleanupTimestamp(LocalDateTime.parse("1999-05-05T10:41:20"));
    testMetadataA.setCreationTimestamp(LocalDateTime.parse("1999-05-05T10:41:20"));

    for (HousekeepingMetadata testMetadata : Arrays.asList(testMetadataA, testMetadataD, testMetadataE)) {
      insertExpiredMetadata(testMetadata);
    }

    String filters = "?housekeeping_status=FAILED"
        + "&lifecycle_type=EXPIRED"
        + "&deleted_before=2000-05-05T10:41:20"
        + "&registered_before=2000-04-04T10:41:20"
        + "&path=s3://some/path/event_date=2020-01-01/event_hour=0/event_type=A"
        + "&partition_name=event_date=2020-01-01/event_hour=0/event_type=A";

    HttpResponse<String> response = testClient.getMetadata(someDatabase, someTable, filters);
    assertThat(response.statusCode()).isEqualTo(OK.value());
    String body = response.body();
    Page<HousekeepingMetadataResponse> responsePage = mapper
        .readValue(body, new TypeReference<RestResponsePage<HousekeepingMetadataResponse>>() {});
    List<HousekeepingMetadataResponse> result = responsePage.getContent();

    assertThatMetadataEqualsResponse(testMetadataA, result.get(0));
    assertThat(result).hasSize(1);
  }

  @Test
  public void testGetPathsWhenThereIsFiltering() throws SQLException, InterruptedException, IOException {
    testPathA.setCleanupTimestamp(LocalDateTime.parse("1999-05-05T10:41:20"));
    testPathA.setCreationTimestamp(LocalDateTime.parse("1999-05-05T10:41:20"));

    for (HousekeepingPath testPath : Arrays.asList(testPathA, testPathB, testPathC)) {
      insertUnreferencedPath(testPath);
    }
    String filters = "?housekeeping_status=FAILED"
        + "&lifecycle_type=EXPIRED"
        + "&deleted_before=2000-05-05T10:41:20"
        + "&registered_before=2000-04-04T10:41:20"
        + "&path=s3://some/path/event_date=2020-01-01/event_hour=0/event_type=A";

    HttpResponse<String> response = testClient.getUnreferencedPaths(someDatabase, someTable, filters);
    assertThat(response.statusCode()).isEqualTo(OK.value());
    String body = response.body();
    Page<HousekeepingPathResponse> responsePage = mapper
        .readValue(body, new TypeReference<RestResponsePage<HousekeepingPathResponse>>() {});
    List<HousekeepingPathResponse> result = responsePage.getContent();
    assertThat(responsePage.getTotalElements()).isEqualTo(1L);
    assertThatPathsEqualsResponse(testPathA, result.get(0));
    assertThat(result).hasSize(1);
  }

  @Test
  public void testPathsPageable() throws SQLException, InterruptedException, IOException {
    for (HousekeepingPath testPath : Arrays.asList(testPathA, testPathB, testPathC)) {
      insertUnreferencedPath(testPath);
    }

    String filters = "?housekeeping_status=FAILED&page=1&size=" + pageSize;
    HttpResponse<String> response = testClient.getUnreferencedPaths(someDatabase, someTable, filters);

    assertThat(response.statusCode()).isEqualTo(OK.value());
    String body = response.body();
    Page<HousekeepingPathResponse> responsePage = mapper
        .readValue(body, new TypeReference<RestResponsePage<HousekeepingPathResponse>>() {});
    List<HousekeepingPathResponse> result = responsePage.getContent();
    assertThat(result).hasSize(1);
    assertThat(responsePage.getTotalElements()).isEqualTo(3L);
    assertThat(responsePage.getTotalPages()).isEqualTo(3L);
  }

  @Test
  public void testMetadataPageable() throws SQLException, InterruptedException, IOException {
    for (HousekeepingMetadata testMetadata : Arrays.asList(testMetadataA, testMetadataB, testMetadataC)) {
      insertExpiredMetadata(testMetadata);
    }

    String filters = "?housekeeping_status=FAILED&page=1&size=" + pageSize;
    HttpResponse<String> response = testClient.getMetadata(someDatabase, someTable, filters);

    assertThat(response.statusCode()).isEqualTo(OK.value());
    String body = response.body();
    Page<HousekeepingMetadataResponse> responsePage = mapper
        .readValue(body, new TypeReference<RestResponsePage<HousekeepingMetadataResponse>>() {});
    List<HousekeepingMetadataResponse> result = responsePage.getContent();
    assertThat(result).hasSize(1);
    assertThat(responsePage.getTotalElements()).isEqualTo(3L);
    assertThat(responsePage.getTotalPages()).isEqualTo(3L);
  }

  // This test is to manually test the API
  @Disabled
  @Test
  public void manualTest() throws SQLException, InterruptedException {
    for (HousekeepingMetadata testMetadata : Arrays.asList(testMetadataA, testMetadataB, testMetadataC)) {
      insertExpiredMetadata(testMetadata);
    }

    for (HousekeepingPath testPath : Arrays.asList(testPathA, testPathB, testPathC)) {
      insertUnreferencedPath(testPath);
    }

    Thread.sleep(10000000L);
  }

  private void assertThatMetadataEqualsResponse(
      HousekeepingMetadata housekeepingMetadata,
      HousekeepingMetadataResponse housekeepingMetadataResponse) {
    assertThat(housekeepingMetadata.getDatabaseName()).isEqualTo(housekeepingMetadataResponse.getDatabaseName());
    assertThat(housekeepingMetadata.getTableName()).isEqualTo(housekeepingMetadataResponse.getTableName());
    assertThat(housekeepingMetadata.getPath()).isEqualTo(housekeepingMetadataResponse.getPath());
    assertThat(housekeepingMetadata.getHousekeepingStatus())
        .isEqualTo(housekeepingMetadataResponse.getHousekeepingStatus());
    assertThat(housekeepingMetadata.getCleanupDelay().toString())
        .isEqualTo(housekeepingMetadataResponse.getCleanupDelay());
    assertThat(housekeepingMetadata.getCleanupAttempts()).isEqualTo(housekeepingMetadataResponse.getCleanupAttempts());
    assertThat(housekeepingMetadata.getLifecycleType()).isEqualTo(housekeepingMetadataResponse.getLifecycleType());
  }

  private void assertThatPathsEqualsResponse(
      HousekeepingPath housekeepingPath,
      HousekeepingPathResponse housekeepingPathResponse) {
    assertThat(housekeepingPath.getDatabaseName()).isEqualTo(housekeepingPathResponse.getDatabaseName());
    assertThat(housekeepingPath.getTableName()).isEqualTo(housekeepingPathResponse.getTableName());
    assertThat(housekeepingPath.getPath()).isEqualTo(housekeepingPathResponse.getPath());
    assertThat(housekeepingPath.getHousekeepingStatus()).isEqualTo(housekeepingPathResponse.getHousekeepingStatus());
    assertThat(housekeepingPath.getCleanupDelay().toString()).isEqualTo(housekeepingPathResponse.getCleanupDelay());
    assertThat(housekeepingPath.getCleanupAttempts()).isEqualTo(housekeepingPathResponse.getCleanupAttempts());
    assertThat(housekeepingPath.getLifecycleType()).isEqualTo(housekeepingPathResponse.getLifecycleType());
  }

  private HousekeepingMetadata createHousekeepingMetadata(
      String tableName,
      String path,
      String partitionName,
      LifecycleEventType lifecycleEventType,
      String cleanupDelay,
      HousekeepingStatus housekeepingStatus) {
    return HousekeepingMetadata
        .builder()
        .id(id++)
        .path(path)
        .databaseName(DATABASE_NAME_VALUE)
        .tableName(tableName)
        .partitionName(partitionName)
        .housekeepingStatus(housekeepingStatus)
        .creationTimestamp(CREATION_TIMESTAMP_VALUE)
        .modifiedTimestamp(CREATION_TIMESTAMP_VALUE)
        .cleanupDelay(PeriodDuration.parse(cleanupDelay))
        .cleanupAttempts(CLEANUP_ATTEMPTS_VALUE)
        .lifecycleType(lifecycleEventType.toString())
        .clientId(CLIENT_ID_FIELD)
        .build();
  }

  private HousekeepingPath createHousekeepingPath(
      String tableName,
      String path,
      LifecycleEventType lifecycleEventType,
      String cleanupDelay,
      HousekeepingStatus housekeepingStatus) {
    return HousekeepingPath
        .builder()
        .id(id++)
        .path(path)
        .databaseName(DATABASE_NAME_VALUE)
        .tableName(tableName)
        .housekeepingStatus(housekeepingStatus)
        .creationTimestamp(CREATION_TIMESTAMP_VALUE)
        .modifiedTimestamp(CREATION_TIMESTAMP_VALUE)
        .cleanupDelay(PeriodDuration.parse(cleanupDelay))
        .cleanupAttempts(CLEANUP_ATTEMPTS_VALUE)
        .lifecycleType(lifecycleEventType.toString())
        .clientId(CLIENT_ID_FIELD)
        .build();
  }

  @Test
  public void testInvalidSortParameter() throws SQLException, IOException, InterruptedException {
    insertExpiredMetadata(testMetadataA);

    String filters = "?sort=nonExistentProperty,asc";
    HttpResponse<String> response = testClient.getMetadata(someDatabase, someTable, filters);

    assertThat(response.statusCode()).isEqualTo(HttpStatus.BAD_REQUEST.value());

    String body = response.body();
    ErrorResponse errorResponse = mapper.readValue(body, ErrorResponse.class);

    assertThat(errorResponse.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST.value());
    assertThat(errorResponse.getMessage()).isEqualTo("No property 'nonExistentProperty' found for type 'HousekeepingMetadata'");
    assertThat(errorResponse.getError()).isEqualTo("Bad Request");
    assertThat(errorResponse.getPath()).contains("/api/v1/database/some_database/table/some_table/metadata");
    assertThat(errorResponse.getTimestamp()).isNotNull();
  }

}
