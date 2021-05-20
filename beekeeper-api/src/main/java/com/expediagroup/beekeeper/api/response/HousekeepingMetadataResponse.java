package com.expediagroup.beekeeper.api.response;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Table;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;

import lombok.Builder;
import lombok.Value;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;

@Value
@Builder
@Table(name = "housekeeping_metadata")
public class HousekeepingMetadataResponse {

  @Column(name = "database_name", nullable = false)
  String databaseName;

  @Column(name = "table_name", nullable = false)
  String tableName;

  @Column(name = "path", nullable = false)
  String path;

  @Column(name = "lifecycles", nullable = false)
  List<Lifecycle> lifecycles;

  public static HousekeepingMetadataResponse convertToHouseKeepingMetadataResponse(
      HousekeepingMetadata housekeepingMetadata) {

    List<Lifecycle> lifecyclesTempList = new ArrayList<>();

    String lifecycleType = housekeepingMetadata.getLifecycleType();

    Lifecycle lifecycle = Lifecycle.builder()
        .lifecycleEventType(lifecycleType)
        .build()
        ;
    lifecyclesTempList.add(lifecycle);

    return HousekeepingMetadataResponse.builder()
        .databaseName(housekeepingMetadata.getDatabaseName())
        .tableName(housekeepingMetadata.getTableName())
        .path(housekeepingMetadata.getPath())
        .lifecycles(lifecyclesTempList)
        .build();
  }

  public static Page<HousekeepingMetadataResponse> convertToHouseKeepingMetadataResponsePage(List<HousekeepingMetadata> housekeepingMetadataList){
    List<HousekeepingMetadataResponse> housekeepingMetadataResponseList = new ArrayList<>();
    for (HousekeepingMetadata housekeepingMetadata : housekeepingMetadataList) {
      housekeepingMetadataResponseList.add(convertToHouseKeepingMetadataResponse(housekeepingMetadata));
      //housekeepingMetadataResponseList = checkIfTableExists(housekeepingMetadataResponseList,housekeepingMetadata);
    }


    return new PageImpl<>(housekeepingMetadataResponseList);
  }

  public static List<HousekeepingMetadataResponse> checkIfTableExists(
      List<HousekeepingMetadataResponse> housekeepingMetadataResponseList, HousekeepingMetadata housekeepingMetadata){

    String tableName = housekeepingMetadata.getTableName();
    String databaseName = housekeepingMetadata.getDatabaseName();
    if(!housekeepingMetadataResponseList.isEmpty()) {
      for (HousekeepingMetadataResponse table : housekeepingMetadataResponseList) {
        String tableName2 = table.getTableName();
        String databaseName2 = table.getDatabaseName();
        if (tableName.equals(tableName2) && databaseName.equals(databaseName2)) {
          System.out.println("duplicate table found");
          String lifecycleType = housekeepingMetadata.getLifecycleType();
          Lifecycle lifecycle = Lifecycle.builder()
              .lifecycleEventType(lifecycleType)
              .build();

          table.addLifecycle(lifecycle);
          housekeepingMetadataResponseList.remove(table);
        }
      }
    }
    return housekeepingMetadataResponseList;

  }

  public void addLifecycle(Lifecycle lifecycle){
    lifecycles.add(lifecycle);
  }
}
