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

  private static final String API_ROOT = "/api/v1";
  private static final String DATABASE_AND_TABLE = "/database/some_database/table/some_table";
  private static final String METADATA_PATH = "/metadata";

  private final String getHousekeepingEntityUrl;
  private final HttpClient httpClient;

  public BeekeeperApiTestClient(String baseUrl) {
    this.getHousekeepingEntityUrl = baseUrl + API_ROOT + DATABASE_AND_TABLE;
    this.httpClient = HttpClient.newHttpClient();
  }

  public HttpResponse<String> getMetadata() throws IOException, InterruptedException {
    HttpRequest request = newBuilder().uri(URI.create(getHousekeepingEntityUrl + METADATA_PATH)).GET().build();
    return httpClient.send(request, BodyHandlers.ofString());
  }
  
  public HttpResponse<String> getMetadataWithFiltering(String filters) throws IOException, InterruptedException {
    HttpRequest request = newBuilder().uri(URI.create(getHousekeepingEntityUrl + METADATA_PATH + filters)).GET().build();
    return httpClient.send(request, BodyHandlers.ofString());
  }

}
