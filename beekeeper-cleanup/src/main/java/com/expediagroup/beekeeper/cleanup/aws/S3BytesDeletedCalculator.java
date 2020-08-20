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
package com.expediagroup.beekeeper.cleanup.aws;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3BytesDeletedCalculator {

  private S3Client s3Client;
  private Map<String, Long> keyToSize = new HashMap<>();
  private long bytesDeleted = 0;

  public S3BytesDeletedCalculator(S3Client s3Client) {
    this.s3Client = s3Client;
  }

  public void storeFileSize(String bucket, String key) {
    long bytes = s3Client.getObjectMetadata(bucket, key).getContentLength();
    keyToSize.put(key, bytes);
  }

  public void storeFileSizes(List<S3ObjectSummary> objectSummaries) {
    objectSummaries.forEach(objectSummary -> {
      long bytes = objectSummary.getSize();
      keyToSize.put(objectSummary.getKey(), bytes);
    });
  }

  public void calculateBytesDeleted(List<String> keysDeleted) {
    if (!keyToSize.isEmpty()) {
      keysDeleted.forEach(key -> {
        if (keyToSize.containsKey(key)) {
          bytesDeleted += keyToSize.get(key);
        }
      });
    }
  }

  public long getBytesDeleted() {
    return bytesDeleted;
  }

}
