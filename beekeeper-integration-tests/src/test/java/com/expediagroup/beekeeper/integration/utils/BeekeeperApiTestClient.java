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
package com.expediagroup.beekeeper.integration.utils;

import static java.net.http.HttpRequest.newBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;


public class BeekeeperApiTestClient {
  
  private static final String PREFIX = "/api/v1";
  private static final String TABLES_PATH = PREFIX + "/tables";

  private final String tablesUrl;
  private final HttpClient httpClient;

  public BeekeeperApiTestClient(String baseUrl) {
    this.tablesUrl = baseUrl + TABLES_PATH;
    this.httpClient = HttpClient.newHttpClient();
  }
  
  public HttpResponse<String> getTables() throws IOException, InterruptedException {
    HttpRequest request = newBuilder().uri(URI.create(tablesUrl)).GET().build();
    return httpClient.send(request, BodyHandlers.ofString());
  }
  
  public HttpResponse<String> getTablesWithTableNameFilter(String tableName) throws IOException, InterruptedException {
    HttpRequest request = newBuilder().uri(URI.create(tablesUrl+"?table_name="+tableName)).GET().build();
    return httpClient.send(request, BodyHandlers.ofString());
  }
  
  public HttpResponse<String> getTablesWithDatabaseNameFilter(String databaseName) throws IOException, InterruptedException {
    HttpRequest request = newBuilder().uri(URI.create(tablesUrl+"?database_name="+databaseName)).GET().build();
    return httpClient.send(request, BodyHandlers.ofString());
  }
  
  public HttpResponse<String> getTablesWithHousekeepingStatusFilter(String housekeepingStatus) throws IOException, InterruptedException {
    HttpRequest request = newBuilder().uri(URI.create(tablesUrl+"?housekeeping_status="+housekeepingStatus)).GET().build();
    return httpClient.send(request, BodyHandlers.ofString());
  }
  
  public HttpResponse<String> getTablesWithLifecycleEventTypeFilter(String lifecycleEventType) throws IOException, InterruptedException {
    HttpRequest request = newBuilder().uri(URI.create(tablesUrl+"?lifecycle_type="+lifecycleEventType)).GET().build();
    return httpClient.send(request, BodyHandlers.ofString());
  }
  
  public HttpResponse<String> getTablesWithDeletedBeforeFilter(String timestamp) throws IOException, InterruptedException {
    HttpRequest request = newBuilder().uri(URI.create(tablesUrl+"?deleted_before="+timestamp)).GET().build();
    return httpClient.send(request, BodyHandlers.ofString());
  }

  public HttpResponse<String> getTablesWithDeletedAfterFilter(String timestamp) throws IOException, InterruptedException {
    HttpRequest request = newBuilder().uri(URI.create(tablesUrl+"?deleted_after="+timestamp)).GET().build();
    return httpClient.send(request, BodyHandlers.ofString());
  }

  public HttpResponse<String> getTablesWithRegisteredBeforeFilter(String timestamp) throws IOException, InterruptedException {
    HttpRequest request = newBuilder().uri(URI.create(tablesUrl+"?registered_before="+timestamp)).GET().build();
    return httpClient.send(request, BodyHandlers.ofString());
  }

  public HttpResponse<String> getTablesWithRegisteredAfterFilter(String timestamp) throws IOException, InterruptedException {
    HttpRequest request = newBuilder().uri(URI.create(tablesUrl+"?registered_after="+timestamp)).GET().build();
    return httpClient.send(request, BodyHandlers.ofString());
  }

}
