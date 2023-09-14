/**
 * Copyright (C) 2019-2023 Expedia, Inc.
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
package com.expediagroup.beekeeper.scheduler.apiary.filter;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class LocationNormalizerTest {
  
  private LocationNormalizer normalizer = new LocationNormalizer();

  @Test
  void noChange() {
    assertThat(normalizer.normalize("s3://bucket/prefix")).isEqualTo("s3://bucket/prefix");
    assertThat(normalizer.normalize("/bucket/prefix")).isEqualTo("/bucket/prefix");
    assertThat(normalizer.normalize("hdfs://bucket/prefix")).isEqualTo("hdfs://bucket/prefix");
    assertThat(normalizer.normalize("")).isEqualTo("");
    assertThat(normalizer.normalize(null)).isEqualTo(null);
    assertThat(normalizer.normalize("foo")).isEqualTo("foo");
    assertThat(normalizer.normalize("foo/bar")).isEqualTo("foo/bar");
  }
  
  @Test
  void normalizeTrailingSlash() {
    assertThat(normalizer.normalize("s3://bucket/prefix/")).isEqualTo("s3://bucket/prefix");
    assertThat(normalizer.normalize("s3://bucket/prefix///")).isEqualTo("s3://bucket/prefix");
    assertThat(normalizer.normalize("hdfs://bucket/prefix/")).isEqualTo("hdfs://bucket/prefix");
  }
  
}
