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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.ObjectMetadata;

@ExtendWith(MockitoExtension.class)
class S3BytesDeletedReporterTest {

  private final String content = "content";
  private final String bucket = "bucket";
  private final String key1 = "db/table/id/partition1/file1";
  private final String key2 = "db/table/id/partition1/file2";
  private final String key3 = "db/table/id/partition1/file3";
  private final KeyVersion keyVersion1 = new KeyVersion(key1);
  private final KeyVersion keyVersion2 = new KeyVersion(key2);
  private final KeyVersion keyVersion3 = new KeyVersion(key3);
  private final List<String> allKeys = Arrays.asList(key1, key2, key3);
  private final long size = content.getBytes().length;
  private final DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucket);
  private final ObjectMetadata objectMetadata = new ObjectMetadata();
  private @Mock MeterRegistry meterRegistry;
  private @Mock S3Client s3Client;
  private @Mock Counter counter;
  private S3BytesDeletedReporter s3BytesDeletedReporter;

  @BeforeEach
  void setUp() {
    deleteObjectsRequest.setKeys(Arrays.asList(keyVersion1, keyVersion2, keyVersion3));

    when(meterRegistry.counter(anyString())).thenReturn(counter);
    s3BytesDeletedReporter = new S3BytesDeletedReporter(s3Client, meterRegistry, false);

    objectMetadata.setContentLength(content.getBytes().length);
  }

  @Test
  void typicalForFile() {
    s3BytesDeletedReporter.reportSize(size);
    verify(counter).increment(size);
  }

  @Test
  void typicalForDirectory() {
    when(s3Client.getObjectMetadata(eq(bucket), anyString())).thenReturn(objectMetadata);
    s3BytesDeletedReporter.cacheFileSizes(deleteObjectsRequest);
    s3BytesDeletedReporter.reportDeletedFiles(allKeys);
    verify(counter).increment(size * 3);
  }

  @Test
  void reportDeletedFilesWithNoCachedSizes() {
    s3BytesDeletedReporter.reportDeletedFiles(allKeys);
    verify(counter).increment(0L);
  }

  @Test
  void matchPartialDeletedFiles() {
    when(s3Client.getObjectMetadata(eq(bucket), anyString())).thenReturn(objectMetadata);
    s3BytesDeletedReporter.cacheFileSizes(deleteObjectsRequest);
    s3BytesDeletedReporter.reportDeletedFiles(Collections.singletonList(key1));
    verify(counter).increment(size);
  }

  @Test
  void consecutiveFileSizes() {
    when(s3Client.getObjectMetadata(eq(bucket), anyString())).thenReturn(objectMetadata);
    s3BytesDeletedReporter.cacheFileSizes(deleteObjectsRequest);
    s3BytesDeletedReporter.reportDeletedFiles(allKeys);
    verify(counter).increment(size * 3);

    deleteObjectsRequest.setKeys(Arrays.asList(keyVersion1, keyVersion2));
    s3BytesDeletedReporter.cacheFileSizes(deleteObjectsRequest);
    s3BytesDeletedReporter.reportDeletedFiles(Arrays.asList(key1, key2));
    verify(counter).increment(size * 2);
  }

  @Test
  void noFilesDeleted() {
    when(s3Client.getObjectMetadata(eq(bucket), anyString())).thenReturn(objectMetadata);
    s3BytesDeletedReporter.cacheFileSizes(deleteObjectsRequest);
    s3BytesDeletedReporter.reportDeletedFiles(Collections.emptyList());
    verifyZeroInteractions(counter);
  }
}
