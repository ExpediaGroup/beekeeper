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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;

public class S3BytesDeletedReporter {

  public static final String METRIC_NAME = "s3-bytes-deleted";
  public static final String DRY_RUN_METRIC_NAME = "dry-run-s3-bytes-deleted";
  private static final Logger log = LoggerFactory.getLogger(S3BytesDeletedReporter.class);

  private final S3Client s3Client;
  private final Map<String, Long> keyToSize = new HashMap<>();
  private final Counter counter;

  public S3BytesDeletedReporter(S3Client s3Client, MeterRegistry meterRegistry, boolean dryRunEnabled) {
    this.s3Client = s3Client;

    String metricName = dryRunEnabled ? DRY_RUN_METRIC_NAME : METRIC_NAME;
    counter = meterRegistry.counter(metricName);
  }

  void cacheFileSizes(DeleteObjectsRequest deleteObjectsRequest) {
    keyToSize.clear();

    String bucketName = deleteObjectsRequest.getBucketName();
    deleteObjectsRequest.getKeys().forEach(keyVersion -> {
      String key = keyVersion.getKey();
      long keySize = s3Client.getObjectMetadata(bucketName, key).getContentLength();
      keyToSize.put(key, keySize);
    });
  }

  void reportDeletedFiles(List<String> keys) {
    long totalLength = 0L;
    if (!keys.isEmpty()) {
      for (Map.Entry<String, Long> pair : keyToSize.entrySet()) {
        if (keys.contains(pair.getKey())) {
          totalLength += pair.getValue();
        }
      }
      reportSize(totalLength);
    }
  }

  void reportSize(long size) {
    log.info("deleted {} bytes", size);
    counter.increment(size);
  }

}
