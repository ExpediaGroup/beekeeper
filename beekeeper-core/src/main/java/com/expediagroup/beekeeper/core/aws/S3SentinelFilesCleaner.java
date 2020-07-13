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

import java.util.Optional;

import com.amazonaws.services.s3.AmazonS3URI;

import com.expediagroup.beekeeper.core.path.SentinelFilesCleaner;

public class S3SentinelFilesCleaner implements SentinelFilesCleaner {

  final static String SENTINEL_SUFFIX = "_$folder$";
  private final S3Client s3Client;

  public S3SentinelFilesCleaner(S3Client s3Client) {
    this.s3Client = s3Client;
  }

  @Override
  public void deleteSentinelFiles(String absolutePath) {
    AmazonS3URI s3Path = new AmazonS3URI(absolutePath, true);
    String bucket = s3Path.getBucket();
    String key = s3Path.getKey();

    getSentinelFile(bucket, key).ifPresent(sentinelFile -> s3Client.deleteObject(bucket, sentinelFile));
  }

  private Optional<String> getSentinelFile(String bucket, String key) {
    String sentinelFile = key + SENTINEL_SUFFIX;
    boolean sentinelExists = s3Client.doesObjectExist(bucket, sentinelFile);
    if (sentinelExists) {
      boolean sizeIsZero = s3Client.getObjectMetadata(bucket, sentinelFile).getContentLength() == 0L;
      if (sizeIsZero) {
        return Optional.of(sentinelFile);
      }
    }
    return Optional.empty();
  }

}
