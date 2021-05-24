package com.expediagroup.beekeeper.api.response;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    List<Lifecycle> lifecyclesList = new ArrayList<>();
    Lifecycle lifecycle = Lifecycle.builder()
        .lifecycleEventType(housekeepingMetadata.getLifecycleType())
        .configuration(Map.of(
            "beekeeper.unreferenced.data.retention.period",housekeepingMetadata.getCleanupDelay().toString(),
            "clean up timestamp", housekeepingMetadata.getCleanupTimestamp().toString()
        ))
        .build()
        ;
    lifecyclesList.add(lifecycle);

    return HousekeepingMetadataResponse.builder()
        .databaseName(housekeepingMetadata.getDatabaseName())
        .tableName(housekeepingMetadata.getTableName())
        .path(housekeepingMetadata.getPath())
        .lifecycles(lifecyclesList)
        .build();
  }

  public static Page<HousekeepingMetadataResponse> convertToHouseKeepingMetadataResponsePage(List<HousekeepingMetadata> housekeepingMetadataList){
    List<HousekeepingMetadataResponse> housekeepingMetadataResponseList = new ArrayList<>();
    for (HousekeepingMetadata housekeepingMetadata : housekeepingMetadataList) {
      HousekeepingMetadataResponse housekeepingMetadataResponse = convertToHouseKeepingMetadataResponse(housekeepingMetadata);
      int repeatedTablePosition = checkIfTableExists(housekeepingMetadataResponseList, housekeepingMetadata);

      if(repeatedTablePosition!=-1){
        housekeepingMetadataResponse = housekeepingMetadataResponseList.get(repeatedTablePosition);
        housekeepingMetadataResponseList.remove(repeatedTablePosition);
        Lifecycle lifecycle = Lifecycle.builder()
            .lifecycleEventType(housekeepingMetadata.getLifecycleType())
            .configuration(Map.of(
                "beekeeper.unreferenced.data.retention.period",housekeepingMetadata.getCleanupDelay().toString(),
                "clean up timestamp", housekeepingMetadata.getCleanupTimestamp().toString()
                ))
            .build();
        housekeepingMetadataResponse.addLifecycle(lifecycle);
      }

      housekeepingMetadataResponseList.add(housekeepingMetadataResponse);
    }
    return new PageImpl<>(housekeepingMetadataResponseList);
  }

  public static int checkIfTableExists(
      List<HousekeepingMetadataResponse> housekeepingMetadataResponseList, HousekeepingMetadata housekeepingMetadata){
    int count = -1;
    int positionOfRepeatedTable = -1;
    String tableName1 = housekeepingMetadata.getTableName();
    String databaseName1 = housekeepingMetadata.getDatabaseName();
    if(!housekeepingMetadataResponseList.isEmpty()) {
      for (HousekeepingMetadataResponse table : housekeepingMetadataResponseList) {
        count++;
        String tableName2 = table.getTableName();
        String databaseName2 = table.getDatabaseName();
        if (tableName1.equals(tableName2) && databaseName1.equals(databaseName2)) {
          positionOfRepeatedTable = count;
        }
      }
    }
    return positionOfRepeatedTable;
  }

  public void addLifecycle(Lifecycle lifecycle){
    lifecycles.add(lifecycle);
  }

}
