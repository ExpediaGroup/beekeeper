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
  private static final String SUFIX = "/metadata";

  private final String metadataUrl;
  private final HttpClient httpClient;

  public BeekeeperApiTestClient(String baseUrl) {
    this.metadataUrl = baseUrl + PREFIX;
    this.httpClient = HttpClient.newHttpClient();
  }
  
  public HttpResponse<String> getMetadata() throws IOException, InterruptedException {
    HttpRequest request = newBuilder().uri(URI.create(metadataUrl+"/database/some_database/table/some_table"+SUFIX)).GET().build();
    System.out.println(request);
    return httpClient.send(request, BodyHandlers.ofString());
  }
  
  public HttpResponse<String> getMetadataWithHousekeepingStatusFilter(String housekeepingStatus) throws IOException, InterruptedException {
    HttpRequest request = newBuilder().uri(URI.create(metadataUrl+"/database/some_database/table/some_table"+SUFIX+"?housekeeping_status="+housekeepingStatus)).GET().build();
    return httpClient.send(request, BodyHandlers.ofString());
  }
  
  public HttpResponse<String> getMetadataWithLifecycleEventTypeFilter(String lifecycleEventType) throws IOException, InterruptedException {
    HttpRequest request = newBuilder().uri(URI.create(metadataUrl+"/database/some_database/table/some_table"+SUFIX+"?lifecycle_type="+lifecycleEventType)).GET().build();
    return httpClient.send(request, BodyHandlers.ofString());
  }
  
  public HttpResponse<String> getMetadataWithDeletedBeforeFilter(String timestamp) throws IOException, InterruptedException {
    HttpRequest request = newBuilder().uri(URI.create(metadataUrl+"/database/some_database/table/some_table"+SUFIX+"?deleted_before="+timestamp)).GET().build();
    return httpClient.send(request, BodyHandlers.ofString());
  }

  public HttpResponse<String> getMetadataWithDeletedAfterFilter(String timestamp) throws IOException, InterruptedException {
    HttpRequest request = newBuilder().uri(URI.create(metadataUrl+"/database/some_database/table/some_table"+SUFIX+"?deleted_after="+timestamp)).GET().build();
    return httpClient.send(request, BodyHandlers.ofString());
  }

  public HttpResponse<String> getMetadataWithRegisteredBeforeFilter(String timestamp) throws IOException, InterruptedException {
    HttpRequest request = newBuilder().uri(URI.create(metadataUrl+"/database/some_database/table/some_table"+SUFIX+"?registered_before="+timestamp)).GET().build();
    return httpClient.send(request, BodyHandlers.ofString());
  }

  public HttpResponse<String> getMetadataWithRegisteredAfterFilter(String timestamp) throws IOException, InterruptedException {
    HttpRequest request = newBuilder().uri(URI.create(metadataUrl+"/database/some_database/table/some_table"+SUFIX+"?registered_after="+timestamp)).GET().build();
    return httpClient.send(request, BodyHandlers.ofString());
  }

  public HttpResponse<String> getMetadataWithPathNameFilter(String path) throws IOException, InterruptedException {
    HttpRequest request = newBuilder().uri(URI.create(metadataUrl+"/database/some_database/table/some_table"+SUFIX+"?path_name="+path)).GET().build();
    return httpClient.send(request, BodyHandlers.ofString());
  }

  public HttpResponse<String> getMetadataWithPartitionNameFilter(String path) throws IOException, InterruptedException {
    HttpRequest request = newBuilder().uri(URI.create(metadataUrl+"/database/some_database/table/some_table"+SUFIX+"?partition_name="+path)).GET().build();
    return httpClient.send(request, BodyHandlers.ofString());
  }

}
