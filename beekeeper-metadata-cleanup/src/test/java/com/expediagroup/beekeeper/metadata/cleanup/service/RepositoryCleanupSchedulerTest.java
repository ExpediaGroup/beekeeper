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
package com.expediagroup.beekeeper.metadata.cleanup.service;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.time.Instant;

import org.awaitility.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.expediagroup.beekeeper.cleanup.service.CleanupServiceScheduler;
import com.expediagroup.beekeeper.cleanup.service.RepositoryCleanupScheduler;
import com.expediagroup.beekeeper.cleanup.service.RepositoryCleanupService;
import com.expediagroup.beekeeper.core.error.BeekeeperException;

@ExtendWith(SpringExtension.class)
@ExtendWith(MockitoExtension.class)
@TestPropertySource(properties = {
    "properties.repository-scheduler-cron=*/4 * * * * *" })
@ContextConfiguration(classes = { CleanupServiceScheduler.class, TestConfig.class },
    loader = AnnotationConfigContextLoader.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class RepositoryCleanupSchedulerTest {

  private @Autowired RepositoryCleanupScheduler repositoryCleanupScheduler;
  private @MockBean RepositoryCleanupService repositoryCleanupService;
  private @MockBean CleanupServiceScheduler cleanupServiceScheduler;

  @Test
  void typical() {
    await().atMost(Duration.TEN_SECONDS)
        .untilAsserted(() -> verify(repositoryCleanupService, atLeast(2)).cleanUp(any()));
  }

  @Test
  void cleanupServiceException() {
    doThrow(BeekeeperException.class).when(repositoryCleanupService)
        .cleanUp(any(Instant.class));
    await().atMost(Duration.TEN_SECONDS)
        .untilAsserted(() -> verify(repositoryCleanupService, atLeast(2)).cleanUp(any()));
  }
}
