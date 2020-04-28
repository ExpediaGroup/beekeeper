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
package com.expediagroup.beekeeper.vacuum;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.io.IOException;
import java.util.Collections;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

class ConsistencyCheckTest {

  @Test
  void metastorePathsCorrect() {
    ConsistencyCheck.checkMetastorePaths(Collections.singleton(new Path("/db/table/snapshot/partition")), 4);
  }

  @Test
  void metastorePathsDepthIncorrect() {
    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> ConsistencyCheck.checkMetastorePaths(
            Collections.singleton(new Path("/db/snapshot/partition")), 4));
  }

  @Test
  void metastorePathCorrect() {
    ConsistencyCheck.checkMetastorePath(new Path("/db/table/snapshot/partition"), 4);
  }

  @Test
  void metastorePathDepthIncorrect() {
    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> ConsistencyCheck.checkMetastorePath(
            new Path("/db/snapshot/partition"), 4));
  }

  @Test
  void unvisitedPath() throws IOException {
    Path nonExistent = new Path("/db/table/snapshot/" + RandomStringUtils.randomAlphanumeric(8) + "/partition");
    FileSystem fs = nonExistent.getFileSystem(new Configuration(false));
    ConsistencyCheck.checkUnvisitedPath(fs, nonExistent);
  }

  @Test
  void unvisitedPathExists() throws IOException {
    Path exists = new Path("/tmp");
    FileSystem fs = exists.getFileSystem(new Configuration(false));
    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> ConsistencyCheck.checkUnvisitedPath(fs, exists));
  }
}
