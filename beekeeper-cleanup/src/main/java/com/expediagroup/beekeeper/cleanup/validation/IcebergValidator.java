/**
 * Copyright (C) 2019-2024 Expedia, Inc.
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
package com.expediagroup.beekeeper.cleanup.validation;

import static java.lang.String.format;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expediagroup.beekeeper.cleanup.metadata.CleanerClient;
import com.expediagroup.beekeeper.cleanup.metadata.CleanerClientFactory;
import com.expediagroup.beekeeper.core.error.BeekeeperIcebergException;

public class IcebergValidator {

  private static final Logger log = LoggerFactory.getLogger(IcebergValidator.class);

  private final CleanerClientFactory cleanerClientFactory;

  public IcebergValidator(CleanerClientFactory cleanerClientFactory) {
    this.cleanerClientFactory = cleanerClientFactory;
  }

  /**
   * Beekeeper currently does not support the Iceberg format. Iceberg tables in the Hive Metastore do not store partition information,
   * causing Beekeeper to attempt to clean up the entire table due to the missing information. This method checks if
   * the table is an Iceberg table and throws a BeekeeperIcebergException to stop the process.
   *
   * @param databaseName
   * @param tableName
   */
  public void throwExceptionIfIceberg(String databaseName, String tableName) {
    try (CleanerClient client = cleanerClientFactory.newInstance()) {
      Map<String, String> parameters = client.getTableProperties(databaseName, tableName);
      String tableType = parameters.getOrDefault("table_type", "").toLowerCase();
      String metadataLocation = parameters.getOrDefault("metadata_location", "").toLowerCase();
      if (tableType.contains("iceberg") || !metadataLocation.isEmpty()) {
        throw new BeekeeperIcebergException(
            format("Iceberg table %s.%s is not currently supported in Beekeeper.", databaseName, tableName));
      }
    } catch (Exception e) {
      throw new BeekeeperIcebergException(
          format("Unexpected exception when identifying if table %s.%s is Iceberg.", databaseName, tableName), e);
    }
  }
}
