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
package com.expediagroup.beekeeper.cleanup.path.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.time.Duration;
import java.time.LocalDateTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.findify.s3mock.S3Mock;

import com.amazonaws.services.s3.AmazonS3;

import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;

@ExtendWith(MockitoExtension.class)
class S3DryRunPathCleanerTest {

  private final String content = "Some content";
  private final String bucket = "bucket";
  private final String keyRoot = "table/id1/partition_1";
  private final String key1 = "table/id1/partition_1/file1";
  private final String key2 = "table/id1/partition_1/file2";
  private final String partition1Sentinel = "table/id1/partition_1_$folder$";
  private final String absolutePath = "s3://" + bucket + "/" + keyRoot;
  private final String tableName = "table";
  private final String databaseName = "database";

  private final S3Mock s3Mock = new S3Mock.Builder().withPort(0).withInMemoryBackend().build();
  private EntityHousekeepingPath housekeepingPath;
  private AmazonS3 amazonS3;
  private S3Client s3Client;
  private @Mock S3BytesDeletedReporter s3BytesDeletedReporter;

  private S3PathCleaner s3DryRunPathCleaner;

  @BeforeEach
  void setUp() {
    amazonS3 = AmazonS3Factory.newInstance(s3Mock);
    amazonS3.createBucket(bucket);
    s3Client = new S3Client(amazonS3, true);
    s3DryRunPathCleaner = new S3PathCleaner(s3Client, new S3SentinelFilesCleaner(s3Client), s3BytesDeletedReporter);
    housekeepingPath = new EntityHousekeepingPath.Builder()
      .path(absolutePath)
      .tableName(tableName)
      .databaseName(databaseName)
      .creationTimestamp(LocalDateTime.now())
      .cleanupDelay(Duration.ofDays(1))
      .build();
  }

  @AfterEach
  void tearDown() {
    s3Mock.shutdown();
  }

  @Test
  void typicalForDirectory() {
    amazonS3.putObject(bucket, key1, content);
    amazonS3.putObject(bucket, key2, content);

    s3DryRunPathCleaner.cleanupPath(housekeepingPath);

    assertThat(amazonS3.doesObjectExist(bucket, key1)).isTrue();
    assertThat(amazonS3.doesObjectExist(bucket, key2)).isTrue();
  }

  @Test
  void directoryWithTrailingSlash() {
    amazonS3.putObject(bucket, key1, content);
    amazonS3.putObject(bucket, key2, content);

    String directoryPath = absolutePath + "/";
    housekeepingPath.setPath(directoryPath);
    s3DryRunPathCleaner.cleanupPath(housekeepingPath);

    assertThat(amazonS3.doesObjectExist(bucket, key1)).isTrue();
    assertThat(amazonS3.doesObjectExist(bucket, key2)).isTrue();
  }

  @Test
  void typicalForFile() {
    amazonS3.putObject(bucket, key1, content);
    amazonS3.putObject(bucket, key2, content);

    String absoluteFilePath = "s3://" + bucket + "/" + key1;
    housekeepingPath.setPath(absoluteFilePath);
    s3DryRunPathCleaner.cleanupPath(housekeepingPath);

    assertThat(amazonS3.doesObjectExist(bucket, key1)).isTrue();
    assertThat(amazonS3.doesObjectExist(bucket, key2)).isTrue();
  }

  @Test
  void typicalWithSentinelFile() {
    String partition1Sentinel = "table/id1/partition_1_$folder$";
    amazonS3.putObject(bucket, partition1Sentinel, "");
    amazonS3.putObject(bucket, key1, content);
    amazonS3.putObject(bucket, key2, content);

    s3DryRunPathCleaner.cleanupPath(housekeepingPath);

    assertThat(amazonS3.doesObjectExist(bucket, key1)).isTrue();
    assertThat(amazonS3.doesObjectExist(bucket, key2)).isTrue();
    assertThat(amazonS3.doesObjectExist(bucket, partition1Sentinel)).isTrue();
  }


  @Test
  void typicalWithAnotherFolderAndSentinelFile() {
    String partition10Sentinel = "table/id1/partition_10_$folder$";
    String partition10File = "table/id1/partition_10/data.file";
    assertThat(amazonS3.doesBucketExistV2(bucket)).isTrue();
    amazonS3.putObject(bucket, key1, content);
    amazonS3.putObject(bucket, key2, content);
    amazonS3.putObject(bucket, partition1Sentinel, "");
    amazonS3.putObject(bucket, partition10File, content);
    amazonS3.putObject(bucket, partition10Sentinel, "");

    s3DryRunPathCleaner.cleanupPath(housekeepingPath);

    assertThat(amazonS3.doesObjectExist(bucket, key1)).isTrue();
    assertThat(amazonS3.doesObjectExist(bucket, key2)).isTrue();
    assertThat(amazonS3.doesObjectExist(bucket, partition1Sentinel)).isTrue();
    assertThat(amazonS3.doesObjectExist(bucket, partition10File)).isTrue();
    assertThat(amazonS3.doesObjectExist(bucket, partition10Sentinel)).isTrue();
  }

  @Test
  void typicalWithParentSentinelFiles() {
    String parentSentinelFile = "table/id1_$folder$";
    String tableSentinelFile = "table_$folder$";
    assertThat(amazonS3.doesBucketExistV2(bucket)).isTrue();
    amazonS3.putObject(bucket, key1, content);
    amazonS3.putObject(bucket, key2, content);
    amazonS3.putObject(bucket, partition1Sentinel, "");
    amazonS3.putObject(bucket, parentSentinelFile, "");
    amazonS3.putObject(bucket, tableSentinelFile, "");

    s3DryRunPathCleaner.cleanupPath(housekeepingPath);

    assertThat(amazonS3.doesObjectExist(bucket, key1)).isTrue();
    assertThat(amazonS3.doesObjectExist(bucket, key2)).isTrue();
    assertThat(amazonS3.doesObjectExist(bucket, partition1Sentinel)).isTrue();
    assertThat(amazonS3.doesObjectExist(bucket, parentSentinelFile)).isTrue();
    assertThat(amazonS3.doesObjectExist(bucket, tableSentinelFile)).isTrue();
  }

  @Test
  void deleteTable() {
    String parentSentinelFile = "table/id1_$folder$";
    String tableSentinelFile = "table_$folder$";
    assertThat(amazonS3.doesBucketExistV2(bucket)).isTrue();
    amazonS3.putObject(bucket, key1, content);
    amazonS3.putObject(bucket, key2, content);
    amazonS3.putObject(bucket, partition1Sentinel, "");
    amazonS3.putObject(bucket, parentSentinelFile, "");
    amazonS3.putObject(bucket, tableSentinelFile, "");

    String tableAbsolutePath = "s3://" + bucket + "/table";
    housekeepingPath.setPath(tableAbsolutePath);
    s3DryRunPathCleaner.cleanupPath(housekeepingPath);

    assertThat(amazonS3.doesObjectExist(bucket, key1)).isTrue();
    assertThat(amazonS3.doesObjectExist(bucket, key2)).isTrue();
    assertThat(amazonS3.doesObjectExist(bucket, partition1Sentinel)).isTrue();
    assertThat(amazonS3.doesObjectExist(bucket, parentSentinelFile)).isTrue();
    assertThat(amazonS3.doesObjectExist(bucket, tableSentinelFile)).isTrue();
  }

  @Test
  void pathDoesNotExist() {
    assertThatCode(() -> s3DryRunPathCleaner.cleanupPath(housekeepingPath)).doesNotThrowAnyException();
  }

}
