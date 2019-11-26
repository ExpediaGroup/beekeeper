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

import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.annotation.Timed;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Strings;

import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.cleanup.path.SentinelFilesCleaner;
import com.expediagroup.beekeeper.core.error.BeekeeperException;

public class S3PathCleaner implements PathCleaner {

  private static final Logger log = LoggerFactory.getLogger(S3PathCleaner.class);
  private final S3Client s3Client;
  private final SentinelFilesCleaner sentinelFilesCleaner;
  private final S3BytesDeletedReporter s3BytesDeletedReporter;

  public S3PathCleaner(S3Client s3Client, SentinelFilesCleaner sentinelFilesCleaner,
      S3BytesDeletedReporter s3BytesDeletedReporter) {
    this.s3Client = s3Client;
    this.sentinelFilesCleaner = sentinelFilesCleaner;
    this.s3BytesDeletedReporter = s3BytesDeletedReporter;
  }

  @Override
  @Timed("s3-paths-deleted")
  public void cleanupPath(String housekeepingPath, String tableName) {
    S3SchemeURI s3URI = extractURI(housekeepingPath);
    String key = s3URI.getKey();
    String bucket = s3URI.getBucket();

    // doesObjectExists returns true only when the key is a file
    boolean isFile = s3Client.doesObjectExist(bucket, key);

    if (isFile) {
      long size = s3Client.getObjectMetadata(bucket, key).getContentLength();
      s3Client.deleteObject(bucket, key);
      s3BytesDeletedReporter.reportSize(size);
    } else {
      deleteFilesInDirectory(bucket, key);

      try {
        if (housekeepingPath.endsWith("/")) {
          housekeepingPath = housekeepingPath.substring(0, housekeepingPath.length() - 1);
        }
        sentinelFilesCleaner.deleteSentinelFiles(housekeepingPath);

        // attempt to delete parents if there is at least one parent
        if (key.contains("/")) {
          deleteParentSentinelFiles(bucket, key, housekeepingPath, tableName);
        }
      } catch (Exception e) {
        log.warn("Sentinel file(s) could not be deleted", e);
      }
    }
  }

  private S3SchemeURI extractURI(String housekeepingPath) {
    S3SchemeURI s3SchemeUri;
    try {
      s3SchemeUri = new S3SchemeURI(housekeepingPath);
    } catch (Exception e) {
      throw new BeekeeperException(format("Could not create URI from path: '%s'", housekeepingPath), e);
    }
    return s3SchemeUri;
  }

  private void deleteFilesInDirectory(String bucket, String key) {
    if (!key.endsWith("/")) {
      key += "/";
    }

    List<S3ObjectSummary> objectsWithKeyAsPrefix = s3Client.listObjects(bucket, key);
    int totalFiles = objectsWithKeyAsPrefix.size();
    int successfulDeletes = deleteAllObjectsAtKey(bucket, objectsWithKeyAsPrefix);

    if (totalFiles != successfulDeletes) {
      throw new BeekeeperException(
          format("Not all files could be deleted at path \"%s/%s\"; deleted %s/%s objects", bucket, key,
              successfulDeletes, totalFiles));
    }
  }

  private int deleteAllObjectsAtKey(String bucket, List<S3ObjectSummary> objectsWithKeyAsPrefix) {
    List<DeleteObjectsRequest.KeyVersion> deleteObjectsRequestKeys = getKeysToDelete(objectsWithKeyAsPrefix);

    DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucket);
    deleteObjectsRequest.setKeys(deleteObjectsRequestKeys);

    s3BytesDeletedReporter.cacheFileSizes(deleteObjectsRequest);
    List<String> deletedKeys = s3Client.deleteObjects(deleteObjectsRequest);
    s3BytesDeletedReporter.reportDeletedFiles(deletedKeys);

    return deletedKeys.size();
  }

  private List<DeleteObjectsRequest.KeyVersion> getKeysToDelete(List<S3ObjectSummary> objectSummaries) {
    return objectSummaries.stream()
        .map(objectSummary -> new DeleteObjectsRequest.KeyVersion(objectSummary.getKey()))
        .collect(Collectors.toList());
  }

  private void deleteParentSentinelFiles(String bucket, String key, String absolutePath, String tableName) {
    String parentPath = absolutePath.substring(0, absolutePath.lastIndexOf("/"));
    String parentKey = key.substring(0, key.lastIndexOf("/"));

    if (pathHasValidTableName(parentPath, tableName) && s3Client.isEmpty(bucket, parentKey, key)) {
      sentinelFilesCleaner.deleteSentinelFiles(parentPath);

      if (parentKey.contains("/")) {
        deleteParentSentinelFiles(bucket, parentKey, parentPath, tableName);
      }
    }
  }

  // stop deleting if the path doesn't contain the table name or we got to the table directory
  private boolean pathHasValidTableName(String parent, String tableName) {
    String tableDirectory = "/" + tableName + "/";
    return !Strings.isNullOrEmpty(tableName) && parent.contains(tableDirectory) && !parent.endsWith("/" + tableName);
  }

}
