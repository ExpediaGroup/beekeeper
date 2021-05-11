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
package com.expediagroup.beekeeper.cleanup.aws;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import cloud.localstack.ServiceName;
import cloud.localstack.awssdkv1.TestUtils;
import cloud.localstack.docker.LocalstackDockerExtension;
import cloud.localstack.docker.annotation.LocalstackDockerProperties;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import com.expediagroup.beekeeper.cleanup.monitoring.BytesDeletedReporter;
import com.expediagroup.beekeeper.core.config.FileSystemType;
import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;

@ExtendWith(MockitoExtension.class)
@ExtendWith(LocalstackDockerExtension.class)
@LocalstackDockerProperties(services = { ServiceName.S3 })
class S3PathCleanerTest {

  private final String content = "Some content";
  private final String bucket = "bucket";
  private final String keyRoot = "table/id1/partition_1";
  private final String keyRootAsDirectory = keyRoot + "/";
  private final String key1 = "table/id1/partition_1/file1";
  private final String key2 = "table/id1/partition_1/file2";
  private final String partition1Sentinel = "table/id1/partition_1_$folder$";
  private final String absolutePath = "s3://" + bucket + "/" + keyRoot;
  private final String tableName = "table";
  private final String databaseName = "database";

  private HousekeepingPath housekeepingPath;
  private AmazonS3 amazonS3;
  private S3Client s3Client;
  private S3SentinelFilesCleaner s3SentinelFilesCleaner;
  private @Mock BytesDeletedReporter bytesDeletedReporter;
  private boolean dryRunEnabled = false;

  private S3PathCleaner s3PathCleaner;

  @BeforeEach
  void setUp() {
    amazonS3 = TestUtils.getClientS3();
    amazonS3.createBucket(bucket);
    amazonS3.listObjectsV2(bucket)
      .getObjectSummaries()
      .forEach(object -> amazonS3.deleteObject(bucket, object.getKey()));
    s3Client = new S3Client(amazonS3, dryRunEnabled);
    s3SentinelFilesCleaner = new S3SentinelFilesCleaner(s3Client);
    s3PathCleaner = new S3PathCleaner(s3Client, s3SentinelFilesCleaner, bytesDeletedReporter);
    housekeepingPath = HousekeepingPath.builder()
      .path(absolutePath)
      .tableName(tableName)
      .databaseName(databaseName)
      .creationTimestamp(LocalDateTime.now())
      .cleanupDelay(Duration.ofDays(1))
      .build();
  }

  @Test
  void typicalForDirectory() {
    amazonS3.putObject(bucket, key1, content);
    amazonS3.putObject(bucket, key2, content);

    s3PathCleaner.cleanupPath(housekeepingPath);

    assertThat(amazonS3.doesObjectExist(bucket, key1)).isFalse();
    assertThat(amazonS3.doesObjectExist(bucket, key2)).isFalse();
    verify(bytesDeletedReporter).reportTaggable(content.getBytes().length * 2, housekeepingPath, FileSystemType.S3);
  }

  @Test
  void directoryWithSpace() {
    String directoryPath = absolutePath + "/ /";
    housekeepingPath.setPath(directoryPath);
    amazonS3.putObject(bucket, keyRoot + "/ /file1", content);
    amazonS3.putObject(bucket, keyRoot + "/ /file2", content);

    s3PathCleaner.cleanupPath(housekeepingPath);

    assertThat(amazonS3.listObjects(bucket).getObjectSummaries()).isEmpty();
  }

  @Test
  void directoryWithTrailingSlash() {
    amazonS3.putObject(bucket, key1, content);
    amazonS3.putObject(bucket, key2, content);

    String directoryPath = absolutePath + "/";
    housekeepingPath.setPath(directoryPath);
    s3PathCleaner.cleanupPath(housekeepingPath);

    assertThat(amazonS3.doesObjectExist(bucket, key1)).isFalse();
    assertThat(amazonS3.doesObjectExist(bucket, key2)).isFalse();
    verify(bytesDeletedReporter).reportTaggable(content.getBytes().length * 2, housekeepingPath, FileSystemType.S3);
  }

  @Test
  void typicalForFile() {
    amazonS3.putObject(bucket, key1, content);
    amazonS3.putObject(bucket, key2, content);

    String absoluteFilePath = "s3://" + bucket + "/" + key1;
    housekeepingPath.setPath(absoluteFilePath);
    s3PathCleaner.cleanupPath(housekeepingPath);
    assertThat(amazonS3.doesObjectExist(bucket, key1)).isFalse();
    assertThat(amazonS3.doesObjectExist(bucket, key2)).isTrue();
    verify(bytesDeletedReporter).reportTaggable(content.getBytes().length, housekeepingPath, FileSystemType.S3);
  }

  @Test
  void typicalWithSentinelFile() {
    amazonS3.putObject(bucket, partition1Sentinel, "");
    amazonS3.putObject(bucket, key1, content);
    amazonS3.putObject(bucket, key2, content);

    s3PathCleaner.cleanupPath(housekeepingPath);

    assertThat(amazonS3.doesObjectExist(bucket, key1)).isFalse();
    assertThat(amazonS3.doesObjectExist(bucket, key2)).isFalse();
    assertThat(amazonS3.doesObjectExist(bucket, partition1Sentinel)).isFalse();
    verify(bytesDeletedReporter).reportTaggable(content.getBytes().length * 2, housekeepingPath, FileSystemType.S3);
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

    s3PathCleaner.cleanupPath(housekeepingPath);

    assertThat(amazonS3.doesObjectExist(bucket, key1)).isFalse();
    assertThat(amazonS3.doesObjectExist(bucket, key2)).isFalse();
    assertThat(amazonS3.doesObjectExist(bucket, partition1Sentinel)).isFalse();
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

    s3PathCleaner.cleanupPath(housekeepingPath);

    assertThat(amazonS3.doesObjectExist(bucket, key1)).isFalse();
    assertThat(amazonS3.doesObjectExist(bucket, key2)).isFalse();
    assertThat(amazonS3.doesObjectExist(bucket, partition1Sentinel)).isFalse();
    assertThat(amazonS3.doesObjectExist(bucket, parentSentinelFile)).isFalse();
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
    s3PathCleaner.cleanupPath(housekeepingPath);

    assertThat(amazonS3.doesObjectExist(bucket, key1)).isFalse();
    assertThat(amazonS3.doesObjectExist(bucket, key2)).isFalse();
    assertThat(amazonS3.doesObjectExist(bucket, partition1Sentinel)).isFalse();
    assertThat(amazonS3.doesObjectExist(bucket, parentSentinelFile)).isFalse();
    assertThat(amazonS3.doesObjectExist(bucket, tableSentinelFile)).isFalse();
    verify(bytesDeletedReporter).reportTaggable(content.getBytes().length * 2, housekeepingPath, FileSystemType.S3);
  }

  @Test
  void pathDoesNotExist() {
    assertThatCode(() -> s3PathCleaner.cleanupPath(housekeepingPath)).doesNotThrowAnyException();
  }

  @Test
  void sentinelFilesCleanerThrowsException() {
    S3SentinelFilesCleaner s3SentinelFilesCleaner = mock(S3SentinelFilesCleaner.class);
    doThrow(IllegalArgumentException.class).when(s3SentinelFilesCleaner).deleteSentinelFiles(absolutePath);

    amazonS3.putObject(bucket, key1, content);

    s3PathCleaner = new S3PathCleaner(s3Client, s3SentinelFilesCleaner, bytesDeletedReporter);
    assertThatCode(() -> s3PathCleaner.cleanupPath(housekeepingPath)).doesNotThrowAnyException();
    assertThat(amazonS3.doesObjectExist(bucket, key1)).isFalse();
  }

  @Test
  void sentinelFileForTableDirectory() {
    String partitionSentinel = "table/id1/partition_1_$folder$";
    String partitionParentSentinel = "table/id1_$folder$";
    String tableSentinel = "table_$folder$";
    String partitionAbsolutePath = "s3://bucket/table/id1/partition_1";

    amazonS3.putObject(bucket, key1, content);
    amazonS3.putObject(bucket, partitionSentinel, "");
    amazonS3.putObject(bucket, partitionParentSentinel, "");
    amazonS3.putObject(bucket, tableSentinel, "");

    housekeepingPath.setPath(partitionAbsolutePath);
    s3PathCleaner.cleanupPath(housekeepingPath);

    assertThat(amazonS3.doesObjectExist(bucket, key1)).isFalse();
    assertThat(amazonS3.doesObjectExist(bucket, partitionSentinel)).isFalse();
    assertThat(amazonS3.doesObjectExist(bucket, partitionParentSentinel)).isFalse();
    assertThat(amazonS3.doesObjectExist(bucket, tableSentinel)).isTrue();
  }

  @Test
  void sentinelFileForEmptyParent() {
    String partitionSentinel = "table/id1/partition_1_$folder$";
    String partitionParentSentinel = "table/id1_$folder$";
    String partitionAbsolutePath = "s3://bucket/table/id1/partition_1";

    amazonS3.putObject(bucket, partitionSentinel, "");
    amazonS3.putObject(bucket, partitionParentSentinel, "");

    housekeepingPath.setPath(partitionAbsolutePath);
    s3PathCleaner.cleanupPath(housekeepingPath);
    assertThat(amazonS3.doesObjectExist(bucket, partitionSentinel)).isFalse();
    assertThat(amazonS3.doesObjectExist(bucket, partitionParentSentinel)).isFalse();
  }

  @Test
  void sentinelFilesForParentsAndPathWithTrailingSlash() {
    String partitionSentinel = "table/id1/partition_1_$folder$";
    String partitionParentSentinel = "table/id1_$folder$";
    String tableSentinel = "table_$folder$";
    String partitionAbsolutePath = "s3://bucket/table/id1/partition_1";

    amazonS3.putObject(bucket, key1, content);
    amazonS3.putObject(bucket, partitionSentinel, "");
    amazonS3.putObject(bucket, partitionParentSentinel, "");
    amazonS3.putObject(bucket, tableSentinel, "");

    housekeepingPath.setPath(partitionAbsolutePath + "/");
    s3PathCleaner.cleanupPath(housekeepingPath);

    assertThat(amazonS3.doesObjectExist(bucket, key1)).isFalse();
    assertThat(amazonS3.doesObjectExist(bucket, partitionSentinel)).isFalse();
    assertThat(amazonS3.doesObjectExist(bucket, partitionParentSentinel)).isFalse();
    assertThat(amazonS3.doesObjectExist(bucket, tableSentinel)).isTrue();
  }

  @Test
  void noBytesDeletedMetricWhenFileDeletionFails() {
    S3Client mockS3Client = mock(S3Client.class);
    s3PathCleaner = new S3PathCleaner(mockS3Client, s3SentinelFilesCleaner, bytesDeletedReporter);
    when(mockS3Client.doesObjectExist(bucket, key1)).thenReturn(true);
    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(10);
    when(mockS3Client.getObjectMetadata(bucket, key1)).thenReturn(objectMetadata);
    doThrow(AmazonServiceException.class).when(mockS3Client).deleteObject(bucket, key1);

    housekeepingPath.setPath(absolutePath + "/file1");
    assertThatExceptionOfType(AmazonServiceException.class)
        .isThrownBy(() -> s3PathCleaner.cleanupPath(housekeepingPath));
    verifyNoInteractions(bytesDeletedReporter);
  }

  @Test
  void noBytesDeletedMetricWhenDirectoryDeletionFails() {
    S3Client mockS3Client = mock(S3Client.class);
    s3PathCleaner = new S3PathCleaner(mockS3Client, s3SentinelFilesCleaner, bytesDeletedReporter);
    doThrow(AmazonServiceException.class).when(mockS3Client).listObjects(bucket, keyRootAsDirectory);

    assertThatExceptionOfType(AmazonServiceException.class)
        .isThrownBy(() -> s3PathCleaner.cleanupPath(housekeepingPath));
    verifyNoInteractions(bytesDeletedReporter);
  }

  @Test
  void reportBytesDeletedWhenDirectoryDeletionPartiallyFails() {
    AmazonS3 mockAmazonS3 = mock(AmazonS3.class);
    S3Client mockS3Client = new S3Client(mockAmazonS3, false);
    mockOneOutOfTwoObjectsDeleted(mockAmazonS3);
    s3PathCleaner = new S3PathCleaner(mockS3Client, s3SentinelFilesCleaner, bytesDeletedReporter);
    assertThatExceptionOfType(BeekeeperException.class)
      .isThrownBy(() -> s3PathCleaner.cleanupPath(housekeepingPath))
      .withMessage(format("Not all files could be deleted at path \"%s/%s\"; deleted 1/2 objects. "
          + "Objects not deleted: 'table/id1/partition_1/file2'.", bucket,
        keyRootAsDirectory));
    verify(bytesDeletedReporter).reportTaggable(100L, housekeepingPath, FileSystemType.S3);
  }

  @Test
  void extractingURIFails() {
    String path = "not a real path";
    housekeepingPath.setPath(path);
    assertThatExceptionOfType(BeekeeperException.class)
      .isThrownBy(() -> s3PathCleaner.cleanupPath(housekeepingPath))
      .withMessage(format("'%s' is not an S3 path.", path));
  }

  private void mockOneOutOfTwoObjectsDeleted(AmazonS3 mockAmazonS3) {
    S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
    s3ObjectSummary.setBucketName(bucket);
    s3ObjectSummary.setKey(key1);
    s3ObjectSummary.setSize(100L);
    S3ObjectSummary s3ObjectSummary2 = new S3ObjectSummary();
    s3ObjectSummary2.setBucketName(bucket);
    s3ObjectSummary2.setKey(key2);
    s3ObjectSummary2.setSize(50L);
    ListObjectsV2Result listObjectsV2Result = mock(ListObjectsV2Result.class);
    when(listObjectsV2Result.getObjectSummaries()).thenReturn(List.of(s3ObjectSummary, s3ObjectSummary2));
    when(mockAmazonS3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Result);
    DeleteObjectsResult.DeletedObject deletedObject = new DeleteObjectsResult.DeletedObject();
    deletedObject.setKey(key1);
    when(mockAmazonS3.deleteObjects(any(DeleteObjectsRequest.class)))
      .thenReturn(new DeleteObjectsResult(List.of(deletedObject)));
  }
}
