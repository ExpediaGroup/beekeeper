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
package com.expediagroup.beekeeper.cleanup.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
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

import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;

import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;
import com.expediagroup.beekeeper.core.model.PathStatus;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;

@ExtendWith(SpringExtension.class)
@ExtendWith(MockitoExtension.class)
@TestPropertySource(properties = {
    "hibernate.data-source.driver-class-name=org.h2.Driver",
    "hibernate.dialect=org.hibernate.dialect.H2Dialect",
    "hibernate.hbm2ddl.auto=create",
    "spring.datasource.url=jdbc:h2:mem:beekeeper;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE;MODE=MySQL" })
@ContextConfiguration(classes = { TestApplication.class },
    loader = AnnotationConfigContextLoader.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class PagingCleanupServiceTest {

  private final LocalDateTime localNow = LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
  private final String path1 = "s3://test/table/bar/test=1";
  private final String path2 = "hdfs://test/table/bar/test=1/var=1";
  private final String path3 = "s3://test/table/bar/test=1/var=2";
  private final String tableName = "table";
  private EntityHousekeepingPath entityPath1;
  private EntityHousekeepingPath entityPath2;
  private EntityHousekeepingPath entityPath3;
  private @MockBean PathCleaner pathCleaner;
  private @Autowired HousekeepingPathRepository housekeepingPathRepository;
  private PagingCleanupService pagingCleanupService;

  @BeforeEach
  void setUp() {
    entityPath1 = createEntityHousekeepingPath();
    entityPath1.setPath(path1);
    entityPath1.setCleanupTimestamp(localNow);

    entityPath2 = createEntityHousekeepingPath();
    entityPath2.setPath(path2);
    entityPath2.setCleanupTimestamp(localNow);

    entityPath3 = createEntityHousekeepingPath();
    entityPath3.setPath(path3);
    entityPath3.setCleanupTimestamp(localNow);

    pagingCleanupService = new PagingCleanupService(housekeepingPathRepository, pathCleaner, 2, false);
  }

  @Test
  void typicalWithPaging() {
    housekeepingPathRepository.save(entityPath1);
    housekeepingPathRepository.save(entityPath2);
    housekeepingPathRepository.save(entityPath3);

    pagingCleanupService.cleanUp(Instant.now());

    verify(pathCleaner).cleanupPath(entityPath1.getPath(), tableName);
    verify(pathCleaner).cleanupPath(entityPath2.getPath(), tableName);
    verify(pathCleaner).cleanupPath(entityPath3.getPath(), tableName);

    assertOriginalObjectsAreUpdated();

    pagingCleanupService.cleanUp(Instant.now());
    verifyNoMoreInteractions(pathCleaner);
  }

  @Test
  void multiobjectDeletionException() {
    housekeepingPathRepository.save(entityPath1);

    MultiObjectDeleteException.DeleteError deleteError = new MultiObjectDeleteException.DeleteError();
    DeleteObjectsResult.DeletedObject deletedObject = new DeleteObjectsResult.DeletedObject();

    deleteError.setCode("403");
    deleteError.setMessage("EXCEPTION");
    deleteError.setKey("s3://error-bucket");

    deletedObject.setKey("s3://success-bucket");

    List<MultiObjectDeleteException.DeleteError> deleteErrors = List.of(deleteError);
    List<DeleteObjectsResult.DeletedObject> deletedObjects = List.of(deletedObject);

    MultiObjectDeleteException multiObjectDeleteException = new MultiObjectDeleteException(deleteErrors,
      deletedObjects);
    doThrow(multiObjectDeleteException).when(pathCleaner).cleanupPath(any(), any());
    pagingCleanupService.cleanUp(Instant.now());
  }

  private void assertOriginalObjectsAreUpdated() {
    List<EntityHousekeepingPath> result = housekeepingPathRepository.findAll();
    EntityHousekeepingPath housekeepingPath1 = result.get(0);
    EntityHousekeepingPath housekeepingPath2 = result.get(1);
    EntityHousekeepingPath housekeepingPath3 = result.get(2);
    assertThat(housekeepingPath1.getCleanupAttempts()).isEqualTo(1);
    assertThat(housekeepingPath2.getCleanupAttempts()).isEqualTo(1);
    assertThat(housekeepingPath3.getCleanupAttempts()).isEqualTo(1);
    assertThat(housekeepingPath1.getPathStatus()).isEqualTo(PathStatus.DELETED);
    assertThat(housekeepingPath2.getPathStatus()).isEqualTo(PathStatus.DELETED);
    assertThat(housekeepingPath3.getPathStatus()).isEqualTo(PathStatus.DELETED);
  }

  @Test
  void typicalOnePageWithMultipleElements() {
    housekeepingPathRepository.save(entityPath1);
    housekeepingPathRepository.save(entityPath2);

    pagingCleanupService.cleanUp(Instant.now());

    verify(pathCleaner).cleanupPath(entityPath1.getPath(), tableName);
    verify(pathCleaner).cleanupPath(entityPath2.getPath(), tableName);
  }

  @Test
  void mixOfScheduledAndFailedPaths() {
    entityPath2.setPathStatus(PathStatus.FAILED);

    housekeepingPathRepository.save(entityPath1);
    housekeepingPathRepository.save(entityPath2);

    pagingCleanupService.cleanUp(Instant.now());

    verify(pathCleaner).cleanupPath(entityPath1.getPath(), tableName);
    verify(pathCleaner).cleanupPath(entityPath2.getPath(), tableName);
  }

  @Test
  void mixOfAllForPathStatus() {
    entityPath2.setPathStatus(PathStatus.FAILED);
    entityPath3.setPathStatus(PathStatus.DELETED);

    housekeepingPathRepository.save(entityPath1);
    housekeepingPathRepository.save(entityPath2);
    housekeepingPathRepository.save(entityPath3);

    pagingCleanupService.cleanUp(Instant.now());

    verify(pathCleaner).cleanupPath(entityPath1.getPath(), tableName);
    verify(pathCleaner).cleanupPath(entityPath2.getPath(), tableName);

    verifyNoMoreInteractions(pathCleaner);
  }

  @Test
  void pathCleanerException() {
    doThrow(new RuntimeException("Error")).when(pathCleaner).cleanupPath(entityPath1.getPath(), tableName);

    housekeepingPathRepository.save(entityPath1);
    housekeepingPathRepository.save(entityPath2);

    pagingCleanupService.cleanUp(Instant.now());

    verify(pathCleaner).cleanupPath(entityPath1.getPath(), tableName);
    verify(pathCleaner).cleanupPath(entityPath2.getPath(), tableName);

    List<EntityHousekeepingPath> result = housekeepingPathRepository.findAll();
    assertThat(result.size()).isEqualTo(2);
    EntityHousekeepingPath housekeepingPath1 = result.get(0);
    EntityHousekeepingPath housekeepingPath2 = result.get(1);

    assertThat(housekeepingPath1.getPath()).isEqualTo(path1);
    assertThat(housekeepingPath1.getPathStatus()).isEqualTo(PathStatus.FAILED);
    assertThat(housekeepingPath1.getCleanupAttempts()).isEqualTo(1);
    assertThat(housekeepingPath2.getPath()).isEqualTo(path2);
    assertThat(housekeepingPath2.getPathStatus()).isEqualTo(PathStatus.DELETED);
    assertThat(housekeepingPath2.getCleanupAttempts()).isEqualTo(1);
  }

  @Test
  void typicalDryRunWithPaging() {
    pagingCleanupService = new PagingCleanupService(housekeepingPathRepository, pathCleaner, 2, true);
    housekeepingPathRepository.save(entityPath1);
    housekeepingPathRepository.save(entityPath2);
    housekeepingPathRepository.save(entityPath3);
    List<EntityHousekeepingPath> beforeClean = housekeepingPathRepository.findAll();

    pagingCleanupService.cleanUp(Instant.now());
    verify(pathCleaner).cleanupPath(entityPath1.getPath(), tableName);
    verify(pathCleaner).cleanupPath(entityPath2.getPath(), tableName);
    verify(pathCleaner).cleanupPath(entityPath3.getPath(), tableName);

    List<EntityHousekeepingPath> afterClean = housekeepingPathRepository.findAll();
    assertThat(afterClean.get(0)).isEqualToComparingFieldByFieldRecursively(beforeClean.get(0));
    assertThat(afterClean.get(1)).isEqualToComparingFieldByFieldRecursively(beforeClean.get(1));
    assertThat(afterClean.get(2)).isEqualToComparingFieldByFieldRecursively(beforeClean.get(2));
  }

  private EntityHousekeepingPath createEntityHousekeepingPath() {
    return new EntityHousekeepingPath.Builder()
        .path("path")
        .databaseName("database")
        .tableName("table")
        .pathStatus(PathStatus.SCHEDULED)
        .creationTimestamp(localNow)
        .modifiedTimestamp(localNow)
        .cleanupDelay(Duration.parse("P3D"))
        .cleanupAttempts(0)
        .build();
  }
}
