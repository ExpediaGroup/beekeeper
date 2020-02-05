/**
 * Copyright (C) 2019-2020 Expedia, Inc.
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
package com.expediagroup.beekeeper.cleanup.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expediagroup.beekeeper.cleanup.path.aws.S3PathCleaner;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;

@ExtendWith(MockitoExtension.class)
public class UnreferencedHandlerTest {

  @Mock private HousekeepingPathRepository housekeepingPathRepository;
  @Mock private S3PathCleaner s3PathCleaner;

  private UnreferencedHandler handler;

  @BeforeEach
  public void initTest() {
    handler = new UnreferencedHandler(housekeepingPathRepository, s3PathCleaner);
  }

  @Test
  public void verifyPathCleaner() {
    assertThat(handler.getPathCleaner()).isInstanceOf(S3PathCleaner.class);
  }

  @Test
  public void verifyLifecycle() {
    assertThat(handler.getLifecycleType()).isEqualTo(LifecycleEventType.UNREFERENCED);
  }

  @Test
  public void verifyHousekeepingPathFetch() {
    handler.findRecordsToClean(null, null);
    verify(housekeepingPathRepository).findRecordsForCleanupByModifiedTimestamp(null, null);
  }
}
