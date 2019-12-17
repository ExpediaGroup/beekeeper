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
package com.expediagroup.beekeeper.core.model;

import java.util.Map;

public enum CleanupType {
  UNREFERENCED(
          "beekeeper.remove.unreferenced.data", "beekeeper.unreferenced.data.retention.period"),
  EXPIRED("beekeeper.remove.expired.data", "beekeeper.expired.data.retention.period");

  private String tableBooleanParameter;
  private String tableValueParameter;

  CleanupType(String tableBooleanParameter, String tableValueParameter) {
    this.tableBooleanParameter = tableBooleanParameter;
    this.tableValueParameter = tableValueParameter;
  }

  public String tableParameterName() {
    return this.tableBooleanParameter;
  }

  public Boolean getBoolean(Map<String,String> tableParameters) {
    return Boolean.valueOf(tableParameters.get(tableParameterName()));
  }

  public String getValue(Map<String,String> tableParameters) {
    return tableParameters.get(this.tableValueParameter);
  }
}
