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

import org.apache.commons.lang3.StringUtils;

public class S3PathValidator {
  // The minimum number of "/" chars for partition location: s3://basePath/table/partition = 4
  public static boolean validPartitionPath(String location) {
    return getNumSlashes(location) >= 4;
  }

  // The minimum number of "/" chars for table location: s3://basePath/table = 3
  public static boolean validTablePath(String location) {
    return getNumSlashes(location) >= 3;
  }

  private static int getNumSlashes(String location) {
    return StringUtils.countMatches(location, "/");
  }
}
