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

import java.net.URI;
import java.net.URISyntaxException;

import com.amazonaws.services.s3.AmazonS3URI;

import com.expediagroup.beekeeper.core.error.BeekeeperException;

public class S3SchemeURI {

  private AmazonS3URI amazonS3URI;

  public S3SchemeURI(String housekeepingPath) throws URISyntaxException {
    if (!housekeepingPath.startsWith("s3")) {
      throw new BeekeeperException(format("'%s' is not an S3 path.", housekeepingPath));
    }
    URI uri = URI.create(housekeepingPath);
    URI s3Uri = new URI("s3", uri.getHost(), uri.getPath(), null);
    this.amazonS3URI = new AmazonS3URI(s3Uri);
  }

  public String getPath() {
    return amazonS3URI.getURI()
      .toString();
  }

  public String getKey() {
    return amazonS3URI.getKey();
  }

  public String getBucket() {
    return amazonS3URI.getBucket();
  }

}
