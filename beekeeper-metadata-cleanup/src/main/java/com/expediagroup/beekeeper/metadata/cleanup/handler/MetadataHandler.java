/**
 * Copyright (C) 2019-2022 Expedia, Inc.
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
package com.expediagroup.beekeeper.metadata.cleanup.handler;

import java.time.LocalDateTime;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;

public interface MetadataHandler {

  Slice<HousekeepingMetadata> findRecordsToClean(LocalDateTime instant, Pageable pageable);

  void cleanupMetadata(HousekeepingMetadata housekeepingMetadata, LocalDateTime instant, boolean dryRunEnabled);
}
