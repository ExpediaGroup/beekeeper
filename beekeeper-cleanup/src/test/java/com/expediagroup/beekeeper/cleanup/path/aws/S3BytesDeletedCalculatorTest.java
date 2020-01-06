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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.amazonaws.services.s3.model.ObjectMetadata;

import com.expediagroup.beekeeper.core.error.BeekeeperException;

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
    objectMetadata.setContentLength(contentBytes);
    when(s3Client.getObjectMetadata(any(), any())).thenReturn(objectMetadata);
    s3BytesDeletedCalculator = new S3BytesDeletedCalculator(s3Client);
  }

  @Test
  void typicalAllKeysSuccessfullyDeleted() {
    List<String> keys = Arrays.asList(key1, key2, key3);
    s3BytesDeletedCalculator.storeFileSizes(bucket, keys);
    s3BytesDeletedCalculator.calculateBytesDeleted(keys);
    assertThat(s3BytesDeletedCalculator.getBytesDeleted()).isEqualTo(contentBytes * 3);
  }

  @Test
  void noKeysSuccessfullyDeleted() {
    List<String> keys = Arrays.asList(key1, key2, key3);
    s3BytesDeletedCalculator.storeFileSizes(bucket, keys);
    s3BytesDeletedCalculator.calculateBytesDeleted(Collections.emptyList());
    assertThat(s3BytesDeletedCalculator.getBytesDeleted()).isEqualTo(0);
  }

  @Test
  void someKeysSuccessfullyDeleted() {
    List<String> keys = Arrays.asList(key1, key2, key3);
    s3BytesDeletedCalculator.storeFileSizes(bucket, keys);
    s3BytesDeletedCalculator.calculateBytesDeleted(Arrays.asList(key1));
    assertThat(s3BytesDeletedCalculator.getBytesDeleted()).isEqualTo(contentBytes);
  }

  @Test
  void multipleCacheFilesThrowsException() {
    List<String> keys = Arrays.asList(key1, key2, key3);
    List<String> keys2 = Arrays.asList("somekey1", "somekey2", "somekey3");
    s3BytesDeletedCalculator.storeFileSizes(bucket, keys);
    assertThatThrownBy(() -> s3BytesDeletedCalculator.storeFileSizes(bucket, keys2))
      .isInstanceOf(BeekeeperException.class)
      .hasMessage("Should not cache files twice.");
  }
}
