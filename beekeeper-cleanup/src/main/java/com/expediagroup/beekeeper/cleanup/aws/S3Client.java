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
package com.expediagroup.beekeeper.cleanup.aws;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
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

  void deleteObject(String bucket, String key) {
    if (dryRunEnabled) {
      log.info("Dry run - deleting: \"{}/{}\"", bucket, key);
    } else {
      log.info("Deleting \"{}/{}\"", bucket, key);
      amazonS3.deleteObject(bucket, key);
    }
  }

  List<S3ObjectSummary> listObjects(String bucket, String key) {
    List<S3ObjectSummary> objectSummaries = new ArrayList<>();
    ListObjectsV2Result listObjectsV2Result;
    String continuationToken = null;
    do {
      ListObjectsV2Request request = new ListObjectsV2Request()
          .withBucketName(bucket)
          .withPrefix(key)
          .withEncodingType("url")
          .withContinuationToken(continuationToken);
      listObjectsV2Result = amazonS3.listObjectsV2(request);
      objectSummaries.addAll(listObjectsV2Result.getObjectSummaries());
      continuationToken = listObjectsV2Result.getNextContinuationToken();
    } while (listObjectsV2Result.isTruncated());
    return objectSummaries;
  }

  List<String> deleteObjects(String bucket, List<String> keys) {
    if (keys.isEmpty()) {
      return Collections.emptyList();
    }
    DeleteObjectsResult deleteObjectsResult = new DeleteObjectsResult(new ArrayList<>());
    DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucket);
    if (!dryRunEnabled) {
      deleteObjectsRequest.withKeys(keys.toArray(new String[] {}));
      try {
        log.info("Attempting to delete a total of {} objects, from [{}] to [{}]", keys.size(), keys.get(0), keys.get(keys.size()-1));
        deleteObjectsResult = amazonS3.deleteObjects(deleteObjectsRequest);
      } catch (AmazonS3Exception amazonS3Exception) {
        log.error(amazonS3Exception.toString());
        int totalKeys = keys.size();
        int chunkSize = 1000;
        if(totalKeys > chunkSize) {
          log.info("Reattempting by breaking down the request");
          int indexStart;
          int indexEnd = 0;
          deleteObjectsRequest = new DeleteObjectsRequest(bucket);
          while(indexEnd < totalKeys) {
            indexStart = indexEnd;
            indexEnd = indexStart + chunkSize < totalKeys ? indexStart + chunkSize : totalKeys;
            deleteObjectsRequest .withKeys(keys.subList(indexStart, indexEnd).toArray(new String[] {}));
            deleteObjectsResult.getDeletedObjects().addAll(
                    amazonS3.deleteObjects(deleteObjectsRequest).getDeletedObjects());
          }

        }
      }
      // https://github.com/aws/aws-sdk-java/issues/2578
      // https://github.com/aws/aws-sdk-java/issues/1293
      //keys.forEach(key -> log.info("Deleted: \"{}/{}\"", bucket, key));
      log.info("Successfully deleted {} objects", keys.size());
      return deleteObjectsResult.getDeletedObjects()
          .stream()
          .map(DeleteObjectsResult.DeletedObject::getKey)
          .collect(Collectors.toList());
    } else {
      return keys.stream()
          .peek(key -> log.info("Dry run - deleting: \"{}/{}\"", bucket, key))
          .collect(Collectors.toList());
    }
  }

  boolean doesObjectExist(String bucket, String key) {
    return amazonS3.doesObjectExist(bucket, key);
  }

  ObjectMetadata getObjectMetadata(String bucket, String key) {
    return amazonS3.getObjectMetadata(bucket, key);
  }

  boolean isEmpty(String bucket, String key, String leafKey) {
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
