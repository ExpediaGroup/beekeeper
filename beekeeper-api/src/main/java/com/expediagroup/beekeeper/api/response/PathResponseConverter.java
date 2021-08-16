package com.expediagroup.beekeeper.api.response;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;

import com.expediagroup.beekeeper.core.model.HousekeepingPath;

/*
Ideally, this should be done with Spring projections (See https://www.baeldung.com/spring-data-jpa-projections)
but currently projections do not support using Specification (See https://github.com/spring-projects/spring-data-jpa/issues/1378).

Therefore, this is a 'manual' conversion of the HousekeepingPath object, which will be used in the API.
*/

public class PathResponseConverter {

  private static HousekeepingPathResponse convertToHousekeepingPathResponse(
      HousekeepingPath housekeepingPath) {
    return HousekeepingPathResponse
        .builder()
        .path(housekeepingPath.getPath())
        .databaseName(housekeepingPath.getDatabaseName())
        .tableName(housekeepingPath.getTableName())
        .housekeepingStatus(housekeepingPath.getHousekeepingStatus())
        .creationTimestamp(housekeepingPath.getCreationTimestamp())
        .modifiedTimestamp(housekeepingPath.getModifiedTimestamp())
        .cleanupTimestamp(housekeepingPath.getCleanupTimestamp())
        .cleanupDelay(housekeepingPath.getCleanupDelay())
        .cleanupAttempts(housekeepingPath.getCleanupAttempts())
        .lifecycleType(housekeepingPath.getLifecycleType())
        .build();
  }

  public static Page<HousekeepingPathResponse> convertToHousekeepingPathResponsePage(
      Page<HousekeepingPath> housekeepingPathPage) {
    List<HousekeepingPath> housekeepingPathList = housekeepingPathPage.getContent();
    List<HousekeepingPathResponse> housekeepingPathResponseList = new ArrayList<>();
    for (HousekeepingPath housekeepingPath : housekeepingPathList) {
      HousekeepingPathResponse housekeepingPathResponse = convertToHousekeepingPathResponse(
          housekeepingPath);
      housekeepingPathResponseList.add(housekeepingPathResponse);
    }
    return new PageImpl<>(housekeepingPathResponseList);
  }

}