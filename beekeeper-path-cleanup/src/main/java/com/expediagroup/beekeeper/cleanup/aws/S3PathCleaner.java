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

import static java.lang.String.format;

import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Strings;

import com.expediagroup.beekeeper.core.config.FileSystemType;
import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.monitoring.BytesDeletedReporter;
import com.expediagroup.beekeeper.core.monitoring.TimedTaggable;
import com.expediagroup.beekeeper.core.path.PathCleaner;
import com.expediagroup.beekeeper.core.path.SentinelFilesCleaner;

public class S3PathCleaner implements PathCleaner {

  private static final Logger log = LoggerFactory.getLogger(S3PathCleaner.class);

  private S3Client s3Client;
  private SentinelFilesCleaner sentinelFilesCleaner;
  private BytesDeletedReporter bytesDeletedReporter;

  public S3PathCleaner(S3Client s3Client, SentinelFilesCleaner sentinelFilesCleaner,
      BytesDeletedReporter bytesDeletedReporter) {
    this.s3Client = s3Client;
    this.sentinelFilesCleaner = sentinelFilesCleaner;
    this.bytesDeletedReporter = bytesDeletedReporter;
  }

  @Override
  @TimedTaggable("s3-paths-deleted")
  public void cleanupPath(HousekeepingEntity housekeepingEntity) {
    S3SchemeURI s3SchemeURI = new S3SchemeURI(housekeepingEntity.getPath());
    String key = s3SchemeURI.getKey();
    String bucket = s3SchemeURI.getBucket();
    S3BytesDeletedCalculator bytesDeletedCalculator = new S3BytesDeletedCalculator(s3Client);
    boolean isFile = s3Client.doesObjectExist(bucket, key);
    try {
      if (isFile) {
        deleteFile(bucket, key, bytesDeletedCalculator);
      } else {
        deleteFilesInDirectory(bucket, key, bytesDeletedCalculator);
        deleteSentinelFiles(s3SchemeURI, key, bucket, housekeepingEntity.getTableName());
      }
    } finally {
      long bytesDeleted = bytesDeletedCalculator.getBytesDeleted();
      if (bytesDeleted > 0) {
        bytesDeletedReporter.reportTaggable(bytesDeleted, housekeepingEntity, FileSystemType.S3);
      }
    }
  }

  private void deleteFile(String bucket, String key, S3BytesDeletedCalculator bytesDeletedCalculator) {
    bytesDeletedCalculator.storeFileSize(bucket, key);
    s3Client.deleteObject(bucket, key);
    bytesDeletedCalculator.calculateBytesDeleted(List.of(key));
  }

  private void deleteFilesInDirectory(String bucket, String key, S3BytesDeletedCalculator bytesDeletedCalculator) {
    if (!key.endsWith("/")) {
      key += "/";
    }
    List<S3ObjectSummary> objectSummaries = s3Client.listObjects(bucket, key);
    bytesDeletedCalculator.storeFileSizes(objectSummaries);
    List<String> keys = objectSummaries.stream()
      .map(S3ObjectSummary::getKey)
      .collect(Collectors.toList());
    List<String> deletedKeys = s3Client.deleteObjects(bucket, keys);
    bytesDeletedCalculator.calculateBytesDeleted(deletedKeys);
    int totalFiles = keys.size();
    int successfulDeletes = deletedKeys.size();
    if (successfulDeletes != totalFiles) {
      keys.removeAll(deletedKeys);
      String failedDeletions = keys.stream()
        .map(k -> format("'%s'", k))
        .collect(Collectors.joining(", "));
      throw new BeekeeperException(
          format("Not all files could be deleted at path \"%s/%s\"; deleted %s/%s objects. Objects not deleted: %s.",
            bucket, key, successfulDeletes, totalFiles, failedDeletions));
    }
  }

  private void deleteSentinelFiles(S3SchemeURI s3SchemeURI, String key, String bucket, String tableName) {
    try {
      String path = s3SchemeURI.getPath();
      if (path.endsWith("/")) {
        path = path.substring(0, path.length() - 1);
      }
      sentinelFilesCleaner.deleteSentinelFiles(path);

      // attempt to delete parents if there is at least one parent
      if (key.contains("/")) {
        deleteParentSentinelFiles(bucket, key, path, tableName);
      }
    } catch (Exception e) {
      log.warn("Sentinel file(s) could not be deleted", e);
    }
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
