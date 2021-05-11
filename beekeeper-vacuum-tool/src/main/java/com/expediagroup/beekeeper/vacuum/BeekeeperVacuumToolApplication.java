/**
 * Copyright (C) 2019-2021 Expedia, Inc.
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
package com.expediagroup.beekeeper.vacuum;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.google.common.base.Supplier;

import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.scheduler.service.SchedulerService;
import com.expediagroup.beekeeper.vacuum.repository.BeekeeperRepository;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.paths.PathUtils;
import com.hotels.hcommon.hive.metastore.paths.TablePathResolver;

@Component
public class BeekeeperVacuumToolApplication implements ApplicationRunner {

  private static final Logger log = LoggerFactory.getLogger(BeekeeperVacuumToolApplication.class);

  private final SchedulerService schedulerService;
  private final BeekeeperRepository beekeeperRepository;
  private final Supplier<CloseableMetaStoreClient> clientSupplier;
  private final boolean isDryRun;
  private final short batchSize;
  private final String databaseName;
  private final String tableName;
  private final String cleanupDelay;
  private final HiveConf conf;
  private Set<String> housekeepingPaths;
  private IMetaStoreClient metastore;

  @Autowired
  BeekeeperVacuumToolApplication(
      Supplier<CloseableMetaStoreClient> clientSupplier,
      SchedulerService schedulerService,
      BeekeeperRepository beekeeperRepository,
      HiveConf conf,
      @Value("${database}") String databaseName,
      @Value("${table}") String tableName,
      @Value("${default-cleanup-delay:P3D}") String cleanupDelay,
      @Value("${dry-run:false}") boolean isDryRun,
      @Value("${partition-batch-size:1000}") short batchSize) {
    this.clientSupplier = clientSupplier;
    this.schedulerService = schedulerService;
    this.beekeeperRepository = beekeeperRepository;
    this.conf = conf;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.cleanupDelay = cleanupDelay;
    this.isDryRun = isDryRun;
    this.batchSize = batchSize;
  }

  @Override
  public void run(ApplicationArguments args) {
    log.warn("Do not run this tool at the same time as replicating to the target table!");
    if (isDryRun) {
      log.warn("Dry-run only!");
    }
    try {
      metastore = clientSupplier.get();
      housekeepingPaths = fetchHousekeepingPaths(beekeeperRepository);
      vacuumTable(databaseName, tableName);
    } catch (URISyntaxException | TException | IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (metastore != null) {
        metastore.close();
      }
    }
  }

  private Set<String> fetchHousekeepingPaths(BeekeeperRepository repository) {
    log.info("Fetching scheduled paths");
    Set<String> paths = new HashSet<>();
    Iterable<HousekeepingPath> repositoryPaths = repository.findAllScheduledPaths();
    for (HousekeepingPath repositoryPath : repositoryPaths) {
      paths.add(repositoryPath.getPath());
    }
    return paths;
  }

  private void vacuumTable(String databaseName, String tableName) throws TException, URISyntaxException, IOException {
    log.info("Vacuuming table '{}.{}'.", databaseName, tableName);
    Table table = metastore.getTable(databaseName, tableName);
    TablePathResolver pathResolver = TablePathResolver.Factory.newTablePathResolver(metastore, table);
    Path tableBaseLocation = pathResolver.getTableBaseLocation();
    Path globPath = pathResolver.getGlobPath();
    log.debug("Table base location: '{}'", tableBaseLocation);
    log.debug("Glob path: '{}'", globPath);

    Set<Path> metastorePaths = pathResolver.getMetastorePaths(batchSize);
    ConsistencyCheck.checkMetastorePaths(metastorePaths, globPath.depth());
    Set<Path> unvisitedMetastorePaths = new HashSet<>(metastorePaths);

    FileSystem fs = tableBaseLocation.getFileSystem(conf);
    FileStatus[] listStatus = fs.globStatus(globPath, HiddenFileFilter.INSTANCE);
    Set<Path> pathsToRemove = new HashSet<>();

    int metaStorePathCount = 0;
    int housekeepingPathCount = 0;
    for (FileStatus fileStatus : listStatus) {
      Path path = removeTrailingSlash(fileStatus.getPath());
      Path normalisedPath = PathUtils.normalise(path);
      if (metastorePaths.contains(normalisedPath)) {
        log.info("KEEP path '{}', referenced in the metastore.", path);
        unvisitedMetastorePaths.remove(normalisedPath);
        metaStorePathCount++;
      } else if (housekeepingPaths.contains(path.toString())) {
        log.info("KEEP path '{}', referenced in housekeeping.", path);
        housekeepingPathCount++;
      } else {
        pathsToRemove.add(path);
      }
    }
    for (Path unvisitedMetastorePath : unvisitedMetastorePaths) {
      log.warn("Metastore path '{}' references non-existent data!", unvisitedMetastorePath);
      ConsistencyCheck.checkUnvisitedPath(fs, unvisitedMetastorePath);
    }
    long totalBytesConsumed = 0;
    for (Path toRemove : pathsToRemove) {
      ContentSummary contentSummary = fs.getContentSummary(toRemove);
      totalBytesConsumed += contentSummary.getSpaceConsumed();
      removePath(toRemove, databaseName, tableName);
    }

    log.info("Vacuum summary; filesystem: {}, metastore: {}, housekeeping: {}, to remove: {}, bytes: {}.",
        listStatus.length, metaStorePathCount, housekeepingPathCount, pathsToRemove.size(), totalBytesConsumed);
  }

  private Path removeTrailingSlash(Path path) {
    return new Path(StringUtils.stripEnd(path.toString(), "/"));
  }

  private void removePath(Path toRemove, String databaseName, String tableName) {
    log.info("REMOVE path '{}'; it is not referenced and can be deleted.", toRemove);
    if (!isDryRun) {
      schedulerService.scheduleForHousekeeping(HousekeepingPath.builder().databaseName(databaseName)
          .tableName(tableName)
          .path(toRemove.toString())
          .housekeepingStatus(SCHEDULED)
          .lifecycleType(UNREFERENCED.name())
          .creationTimestamp(LocalDateTime.now())
          .cleanupDelay(Duration.parse(cleanupDelay))
          .clientId("beekeeper-vacuum-tool")
          .build());
      log.info("Scheduled path '{}' for deletion.", toRemove.toString());
    } else {
      log.warn("DRY RUN ENABLED: path '{}' left as is.", toRemove);
    }
  }
}
