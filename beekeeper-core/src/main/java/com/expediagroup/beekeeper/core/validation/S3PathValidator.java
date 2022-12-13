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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3PathValidator {

  private static final Logger log = LoggerFactory.getLogger(S3PathValidator.class);

  // The minimum number of "/" chars for partition location: s3://basePath/table/partition = 4
  public static boolean validPartitionPath(String location) {
    boolean valid = getNumSlashes(location) >= 4;
    if (!valid) {
      log.warn("Partition \"{}\" doesn't have the correct number of levels in the path", location);
    }
    return valid;
  }

  // The minimum number of "/" chars for table location: s3://basePath/table = 3
  public static boolean validTablePath(String location) {
    boolean valid = getNumSlashes(location) >= 3;
    if (!valid) {
      log.warn("Table \"{}\" doesn't have the correct number of levels in the path", location);
    }
    return valid;  }

  private static int getNumSlashes(String location) {
    return StringUtils.countMatches(location, "/");
  }
}
