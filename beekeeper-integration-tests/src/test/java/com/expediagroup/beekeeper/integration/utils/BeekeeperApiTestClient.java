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
  private static final String METADATA_PATH = "/metadata";

  private final String housekeepingEntityUrl;
  private final HttpClient httpClient;

  public BeekeeperApiTestClient(String baseUrl) {
    this.housekeepingEntityUrl = baseUrl + API_ROOT;
    this.httpClient = HttpClient.newHttpClient();
  }

  public HttpResponse<String> getMetadata(String database, String table) throws IOException, InterruptedException {
    HttpRequest request = newBuilder()
        .uri(
            URI.create(String.format(housekeepingEntityUrl + "/database/%s/table/%s" + METADATA_PATH, database, table)))
        .GET()
        .build();
    return httpClient.send(request, BodyHandlers.ofString());
  }

  public HttpResponse<String> getMetadata(String database, String table, String filters)
    throws IOException, InterruptedException {
    HttpRequest request = newBuilder()
        .uri(URI
            .create(String
                .format(housekeepingEntityUrl + "/database/%s/table/%s" + METADATA_PATH + "%s", database, table,
                    filters)))
        .GET()
        .build();
    return httpClient.send(request, BodyHandlers.ofString());
  }

}
