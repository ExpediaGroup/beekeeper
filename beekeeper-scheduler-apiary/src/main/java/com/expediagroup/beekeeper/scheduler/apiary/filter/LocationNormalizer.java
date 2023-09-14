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

import org.apache.commons.lang3.StringUtils;

/**
 * Attempts to normalize string representing a location of a Hive Table, could have a wide variety of schemes, s3, s3a,
 * hdfs etc..
 */
public class LocationNormalizer {

  public String normalize(String location) {
    //Not using File.seperator here we might not know what the location would be using. (Beekeeper might run on Windows...)
    location =StringUtils.stripEnd(location, "/");
    return location;
  }

}
