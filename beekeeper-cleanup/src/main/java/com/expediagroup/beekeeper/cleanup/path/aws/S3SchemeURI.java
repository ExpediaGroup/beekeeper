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

import static java.lang.String.format;

import com.amazonaws.services.s3.AmazonS3URI;

import com.expediagroup.beekeeper.core.error.BeekeeperException;

public class S3SchemeURI {

  private static final String S3_SCHEME_REGEX = "^s3(a|n):\\/\\/";

  private AmazonS3URI amazonS3URI;

  public S3SchemeURI(String housekeepingPath) {
    if (!housekeepingPath.startsWith("s3")) {
      throw new BeekeeperException(format("'%s' is not an S3 path.", housekeepingPath));
    }
    String s3Path = housekeepingPath.replaceFirst(S3_SCHEME_REGEX, "s3://");
    this.amazonS3URI = new AmazonS3URI(s3Path);
  }

  public String getPath() {
    return "s3://" + amazonS3URI.getBucket() + "/" + amazonS3URI.getKey();
  }

  public String getKey() {
    return amazonS3URI.getKey();
  }

  public String getBucket() {
    return amazonS3URI.getBucket();
  }

}
