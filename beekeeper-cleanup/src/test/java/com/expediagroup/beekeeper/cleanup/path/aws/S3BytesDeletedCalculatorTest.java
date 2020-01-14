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
package com.expediagroup.beekeeper.cleanup.path.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

@ExtendWith(MockitoExtension.class)
class S3BytesDeletedCalculatorTest {

  private long contentBytes = "content".getBytes().length;
  private String bucket = "bucket";
  private String key1 = "db/table/id/partition1/file1";
  private String key2 = "db/table/id/partition1/file2";
  private String key3 = "db/table/id/partition1/file3";
  private ObjectMetadata objectMetadata = new ObjectMetadata();
  private @Mock S3Client s3Client;
  private S3BytesDeletedCalculator s3BytesDeletedCalculator;

  @BeforeEach
  void setUp() {
    s3BytesDeletedCalculator = new S3BytesDeletedCalculator(s3Client);
  }

  @Test
  void typicalKeySuccessfullyDeleted() {
    objectMetadata.setContentLength(contentBytes);
    when(s3Client.getObjectMetadata(any(), any())).thenReturn(objectMetadata);
    s3BytesDeletedCalculator.storeFileSize(bucket, key1);
    s3BytesDeletedCalculator.calculateBytesDeleted(List.of(key1));
    assertThat(s3BytesDeletedCalculator.getBytesDeleted()).isEqualTo(contentBytes);
  }

  @Test
  void differentKeyDeleted() {
    objectMetadata.setContentLength(contentBytes);
    when(s3Client.getObjectMetadata(any(), any())).thenReturn(objectMetadata);
    s3BytesDeletedCalculator.storeFileSize(bucket, key1);
    s3BytesDeletedCalculator.calculateBytesDeleted(List.of(key2));
    assertThat(s3BytesDeletedCalculator.getBytesDeleted()).isEqualTo(0);
  }

  @Test
  void allObjectsSuccessfullyDeleted() {
    List<S3ObjectSummary> objectSummaries = objectSummaries(key1, key2, key3);
    s3BytesDeletedCalculator.storeFileSizes(objectSummaries);
    s3BytesDeletedCalculator.calculateBytesDeleted(Arrays.asList(key1, key2, key3));
    assertThat(s3BytesDeletedCalculator.getBytesDeleted()).isEqualTo(contentBytes * 3);
  }

  @Test
  void someObjectsSuccessfullyDeleted() {
    List<S3ObjectSummary> objectSummaries = objectSummaries(key1, key2, key3);
    s3BytesDeletedCalculator.storeFileSizes(objectSummaries);
    s3BytesDeletedCalculator.calculateBytesDeleted(Arrays.asList(key1));
    assertThat(s3BytesDeletedCalculator.getBytesDeleted()).isEqualTo(contentBytes);
  }

  @Test
  void noObjectsSuccessfullyDeleted() {
    List<S3ObjectSummary> objectSummaries = objectSummaries(key1, key2, key3);
    s3BytesDeletedCalculator.storeFileSizes(objectSummaries);
    s3BytesDeletedCalculator.calculateBytesDeleted(Collections.emptyList());
    assertThat(s3BytesDeletedCalculator.getBytesDeleted()).isEqualTo(0);
  }

  private List<S3ObjectSummary> objectSummaries(String... keys) {
    return Arrays.stream(keys)
      .map(key -> {
        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
        s3ObjectSummary.setKey(key);
        s3ObjectSummary.setSize(contentBytes);
        return s3ObjectSummary;
      })
      .collect(Collectors.toList());
  }
}
