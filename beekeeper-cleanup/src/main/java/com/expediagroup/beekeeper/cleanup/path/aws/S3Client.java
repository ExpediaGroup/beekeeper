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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3Client {

  private static final Logger log = LoggerFactory.getLogger(S3Client.class);
  private final AmazonS3 amazonS3;
  private final boolean dryRunEnabled;

  public S3Client(AmazonS3 amazonS3, boolean dryRunEnabled) {
    this.amazonS3 = amazonS3;
    this.dryRunEnabled = dryRunEnabled;
  }

  public void deleteObject(String bucket, String key) {
    if (dryRunEnabled) {
      log.info("Dry run - deleting: \"{}/{}\"", bucket, key);
    } else {
      log.info("Deleting \"{}/{}\"", bucket, key);
      amazonS3.deleteObject(bucket, key);
    }
  }

  public List<S3ObjectSummary> listObjects(String bucket, String key) {
    return amazonS3.listObjectsV2(bucket, key).getObjectSummaries();
  }

  public List<String> deleteObjects(DeleteObjectsRequest deleteObjectsRequest) {
    List<DeleteObjectsRequest.KeyVersion> keyVersions = deleteObjectsRequest.getKeys();
    if (keyVersions.isEmpty()) {
      return Collections.emptyList();
    }
    String bucket = deleteObjectsRequest.getBucketName();
    if (!dryRunEnabled) {
      keyVersions.forEach(keyVersion -> log.info("Deleting: \"{}/{}\"", bucket, keyVersion.getKey()));
      DeleteObjectsResult deleteObjectsResult = amazonS3.deleteObjects(deleteObjectsRequest);
      return deleteObjectsResult.getDeletedObjects()
          .stream()
          .map(DeleteObjectsResult.DeletedObject::getKey)
          .collect(Collectors.toList());
    } else {
      return keyVersions
          .stream()
          .map(DeleteObjectsRequest.KeyVersion::getKey)
          .peek(key -> log.info("Dry run - deleting \"{}/{}\"", bucket, key))
          .collect(Collectors.toList());
    }
  }

  public boolean doesObjectExist(String bucket, String key) {
    return amazonS3.doesObjectExist(bucket, key);
  }

  public ObjectMetadata getObjectMetadata(String bucket, String key) {
    return amazonS3.getObjectMetadata(bucket, key);
  }

  public boolean isEmpty(String bucket, String key, String leafKey) {
    List<S3ObjectSummary> objectsLeftAtPath = amazonS3.listObjectsV2(bucket, key + "/").getObjectSummaries();
    if (!dryRunEnabled) {
      return objectsLeftAtPath.size() == 0;
    } else {
      String leafKeySentinel = leafKey + S3SentinelFilesCleaner.SENTINEL_SUFFIX;

      for (S3ObjectSummary s3ObjectSummary : objectsLeftAtPath) {
        String currentKey = s3ObjectSummary.getKey();
        if (!currentKey.startsWith(leafKey + "/") && !currentKey.equals(leafKeySentinel)) {
          return false;
        }
      }
      return true;
    }
  }

}
