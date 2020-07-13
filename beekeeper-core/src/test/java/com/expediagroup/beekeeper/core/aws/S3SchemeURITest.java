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
package com.expediagroup.beekeeper.core.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expediagroup.beekeeper.core.error.BeekeeperException;

@ExtendWith(MockitoExtension.class)
class S3SchemeURITest {

  private static final String BUCKET = "bucket";
  private static final String KEY = "dir/file";
  private static final String S3_PATH = "s3://" + BUCKET + "/" + KEY;
  private static final String S3A_PATH = "s3a://" + BUCKET + "/" + KEY;
  private static final String S3N_PATH = "s3n://" + BUCKET + "/" + KEY;

  @Test
  void typicalPath() {
    S3SchemeURI uri = new S3SchemeURI(S3_PATH);
    assertThat(uri.getPath()).isEqualTo(S3_PATH);
    assertThat(uri.getBucket()).isEqualTo(BUCKET);
    assertThat(uri.getKey()).isEqualTo(KEY);
  }

  @Test
  void pathWithSpace() {
    String pathWithSpace = S3_PATH + "/ /file";
    S3SchemeURI uri = new S3SchemeURI(pathWithSpace);
    assertThat(uri.getPath()).isEqualTo(pathWithSpace);
    assertThat(uri.getBucket()).isEqualTo(BUCKET);
    assertThat(uri.getKey()).isEqualTo(KEY + "/ /file");
  }

  @Test
  void s3aPath() {
    S3SchemeURI uri = new S3SchemeURI(S3A_PATH);
    assertThat(uri.getPath()).isEqualTo(S3_PATH);
    assertThat(uri.getBucket()).isEqualTo(BUCKET);
    assertThat(uri.getKey()).isEqualTo(KEY);
  }

  @Test
  void s3nPath() {
    S3SchemeURI uri = new S3SchemeURI(S3N_PATH);
    assertThat(uri.getPath()).isEqualTo(S3_PATH);
    assertThat(uri.getBucket()).isEqualTo(BUCKET);
    assertThat(uri.getKey()).isEqualTo(KEY);
  }

  @Test
  void pathIsNotS3() {
    assertThatExceptionOfType(BeekeeperException.class)
      .isThrownBy(() -> new S3SchemeURI("anotherscheme://bucket"))
      .withMessage("'anotherscheme://bucket' is not an S3 path.");
  }

}
