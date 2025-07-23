/**
 * Copyright (C) 2019-2025 Expedia, Inc.
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

import static com.expediagroup.beekeeper.integration.CommonTestVariables.DATABASE_NAME_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.EVENT_DETAILS_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.HOUSEKEEPING_STATUS_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.ID_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.LIFECYCLE_TYPE_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.TABLE_NAME_FIELD;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.expediagroup.beekeeper.core.model.history.BeekeeperHistory;

public class ResultSetToBeekeeperHistoryMapper {

  public static BeekeeperHistory mapToBeekeeperHistory(ResultSet resultSet) throws SQLException {
    return BeekeeperHistory.builder()
        .id(resultSet.getLong(ID_FIELD))
        .databaseName(resultSet.getString(DATABASE_NAME_FIELD))
        .tableName(resultSet.getString(TABLE_NAME_FIELD))
        .lifecycleType(resultSet.getString(LIFECYCLE_TYPE_FIELD))
        .housekeepingStatus(resultSet.getString(HOUSEKEEPING_STATUS_FIELD))
        .eventDetails(resultSet.getString(EVENT_DETAILS_FIELD))
        .build();
  }
}
