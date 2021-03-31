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
package com.expediagroup.beekeeper.api;



import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;


@ExtendWith(MockitoExtension.class)
public class ReturnTablesTest {

//  private HousekeepingMetadataRepository housekeepingMetadataRepository;
//  private HousekeepingMetadata table1;
//  private final BeekeeperServiceImpl beekeeperServiceImpl;
//
//  public ReturnTablesTest(
//      HousekeepingMetadataRepository housekeepingMetadataRepository,
//      BeekeeperServiceImpl beekeeperServiceImpl) {this.housekeepingMetadataRepository = housekeepingMetadataRepository;
//    this.beekeeperServiceImpl = beekeeperServiceImpl;
//  }
//
//  @BeforeEach
//  public void createTables(){
//    LocalDateTime CREATION_TIMESTAMP = LocalDateTime.now(ZoneId.of("UTC"));
//    table1 = new HousekeepingMetadata.Builder()
//        .path("s3://some/path/")
//        .databaseName("aRandomDatabase")
//        .tableName("aRandomTable")
//        .partitionName("event_date=2020-01-01/event_hour=0/event_type=A")
//        .housekeepingStatus(SCHEDULED)
//        .creationTimestamp(CREATION_TIMESTAMP)
//        .modifiedTimestamp(CREATION_TIMESTAMP)
//        .cleanupDelay(Duration.parse("P3D"))
//        .cleanupAttempts(0)
//        .lifecycleType(EXPIRED.toString())
//        .build();
//  }
//
//  @Test
//  public void test(){
//    beekeeperServiceImpl.saveTable(table1);
//    List<HousekeepingMetadata> tables = beekeeperServiceImpl.returnAllTables();
//    System.out.println("AAA tables:"+tables.size());
//  }

}
