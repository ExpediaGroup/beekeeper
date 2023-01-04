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
package com.expediagroup.beekeeper.api.response;

import java.time.LocalDateTime;

import org.hibernate.annotations.UpdateTimestamp;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;

import com.expediagroup.beekeeper.core.model.HousekeepingStatus;

@Value
@Builder
public class HousekeepingPathResponse {

  String path;

  String databaseName;

  String tableName;

  HousekeepingStatus housekeepingStatus;

  @EqualsAndHashCode.Exclude
  LocalDateTime creationTimestamp;

  @EqualsAndHashCode.Exclude
  @UpdateTimestamp
  LocalDateTime modifiedTimestamp;

  LocalDateTime cleanupTimestamp;

  String cleanupDelay;

  int cleanupAttempts;

  String lifecycleType;

}
