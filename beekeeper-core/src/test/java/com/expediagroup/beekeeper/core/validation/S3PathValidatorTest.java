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
package com.expediagroup.beekeeper.core.validation;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class S3PathValidatorTest {

  @Test
  void validTablePath() {
    assertThat(S3PathValidator.validTablePath("s3://bucket/table")).isTrue();
    assertThat(S3PathValidator.validTablePath("s3://bucket/table/1")).isTrue();
  }

  @Test
  void invalidTablePath() {
    assertThat(S3PathValidator.validTablePath("s3://bucket")).isFalse();
  }

  @Test
  void validPartitionPath() {
    assertThat(S3PathValidator.validPartitionPath("s3://bucket/table/partition")).isTrue();
    assertThat(S3PathValidator.validPartitionPath("s3://bucket/table/partition/1")).isTrue();
  }

  @Test
  void invalidPartitionPath() {
    assertThat(S3PathValidator.validPartitionPath("s3://bucket/table")).isFalse();
  }

}
