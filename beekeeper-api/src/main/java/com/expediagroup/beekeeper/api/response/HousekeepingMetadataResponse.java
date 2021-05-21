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
      HousekeepingMetadataResponse housekeepingMetadataResponse = convertToHouseKeepingMetadataResponse(housekeepingMetadata);
      int repeatedTablePosition = checkIfTableExists(housekeepingMetadataResponseList, housekeepingMetadata);
      if(!(repeatedTablePosition ==-1)){
        HousekeepingMetadataResponse tampeTable = housekeepingMetadataResponseList.get(repeatedTablePosition);
        housekeepingMetadataResponseList.remove(repeatedTablePosition);
        System.out.println(housekeepingMetadata.toString());
        String lifecycleType = housekeepingMetadata.getLifecycleType();
        System.out.println("lifecycle:"+lifecycleType);
        Lifecycle lifecycle = Lifecycle.builder()
            .lifecycleEventType(lifecycleType)
            .build();
        System.out.println(housekeepingMetadataResponse.toString());

        tampeTable.addLifecycle(lifecycle);
        System.out.println(tampeTable.toString());
        housekeepingMetadataResponseList.add(tampeTable);
      }
      else {
        housekeepingMetadataResponseList.add(housekeepingMetadataResponse);
      }
    }


    return new PageImpl<>(housekeepingMetadataResponseList);
  }

  public static int checkIfTableExists(
      List<HousekeepingMetadataResponse> housekeepingMetadataResponseList, HousekeepingMetadata housekeepingMetadata){

    boolean tableExists = false;
    int count = -1;
    int positionOfRepeatedTable = -1;
    String tableName = housekeepingMetadata.getTableName();
    String databaseName = housekeepingMetadata.getDatabaseName();
    if(!housekeepingMetadataResponseList.isEmpty()) {
      for (HousekeepingMetadataResponse table : housekeepingMetadataResponseList) {
        count++;
        String tableName2 = table.getTableName();
        String databaseName2 = table.getDatabaseName();
        if (tableName.equals(tableName2) && databaseName.equals(databaseName2)) {
          System.out.println("duplicate table foundd");
          tableExists = true;
          positionOfRepeatedTable = count;
//          String lifecycleType = housekeepingMetadata.getLifecycleType();
//          Lifecycle lifecycle = Lifecycle.builder()
//              .lifecycleEventType(lifecycleType)
//              .build();
//
//          table.addLifecycle(lifecycle);
//          housekeepingMetadataResponseList.remove(table);
        }
      }
    }
    return positionOfRepeatedTable;

  }

  public void addLifecycle(Lifecycle lifecycle){
    System.out.println("lifecycle:"+lifecycle);
    System.out.println("lifecycles:"+lifecycles);
    lifecycles.add(lifecycle);
    System.out.println("lifecycle:"+lifecycle);
    System.out.println("lifecycles:"+lifecycles);
  }
}
