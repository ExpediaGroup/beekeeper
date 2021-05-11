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

import static com.expediagroup.beekeeper.integration.CommonTestVariables.CLEANUP_ATTEMPTS_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.CLEANUP_DELAY_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.CLIENT_ID_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.CREATION_TIMESTAMP_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.DATABASE_NAME_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.HOUSEKEEPING_STATUS_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.ID_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.LIFECYCLE_TYPE_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.MODIFIED_TIMESTAMP_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.PARTITION_NAME_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.PATH_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.TABLE_NAME_FIELD;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;

public class ResultSetToHousekeepingEntityMapper {

  public static HousekeepingPath mapToHousekeepingPath(ResultSet resultSet) throws SQLException {
    return HousekeepingPath.builder()
        .id(resultSet.getLong(ID_FIELD))
        .path(resultSet.getString(PATH_FIELD))
        .databaseName(resultSet.getString(DATABASE_NAME_FIELD))
        .tableName(resultSet.getString(TABLE_NAME_FIELD))
        .housekeepingStatus(HousekeepingStatus.valueOf(resultSet.getString(HOUSEKEEPING_STATUS_FIELD)))
        .creationTimestamp(Timestamp.valueOf(resultSet.getString(CREATION_TIMESTAMP_FIELD)).toLocalDateTime())
        .modifiedTimestamp(Timestamp.valueOf(resultSet.getString(MODIFIED_TIMESTAMP_FIELD)).toLocalDateTime())
        .cleanupDelay(Duration.parse(resultSet.getString(CLEANUP_DELAY_FIELD)))
        .cleanupAttempts(resultSet.getInt(CLEANUP_ATTEMPTS_FIELD))
        .clientId(resultSet.getString(CLIENT_ID_FIELD))
        .lifecycleType(resultSet.getString(LIFECYCLE_TYPE_FIELD))
        .build();
  }

  public static HousekeepingMetadata mapToHousekeepingMetadata(ResultSet resultSet) throws SQLException {

    return HousekeepingMetadata.builder()
    .id(resultSet.getLong(ID_FIELD))
    .path(resultSet.getString(PATH_FIELD))
    .databaseName(resultSet.getString(DATABASE_NAME_FIELD))
    .tableName(resultSet.getString(TABLE_NAME_FIELD))
    .partitionName(resultSet.getString(PARTITION_NAME_FIELD))
    .housekeepingStatus(HousekeepingStatus.valueOf(resultSet.getString(HOUSEKEEPING_STATUS_FIELD)))
    .creationTimestamp(Timestamp.valueOf(resultSet.getString(CREATION_TIMESTAMP_FIELD)).toLocalDateTime())
    .modifiedTimestamp(Timestamp.valueOf(resultSet.getString(MODIFIED_TIMESTAMP_FIELD)).toLocalDateTime())
    .cleanupDelay(Duration.parse(resultSet.getString(CLEANUP_DELAY_FIELD)))
    .cleanupAttempts(resultSet.getInt(CLEANUP_ATTEMPTS_FIELD))
    .clientId(resultSet.getString(CLIENT_ID_FIELD))
    .lifecycleType(resultSet.getString(LIFECYCLE_TYPE_FIELD))
    .build();
  }
}
