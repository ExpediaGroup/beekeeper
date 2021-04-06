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

import static org.mockito.Mockito.when;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DELETED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;

import com.google.common.collect.Lists;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;

@ExtendWith(MockitoExtension.class)
public class HouseKeepingEntityServiceImplTest {

  private HousekeepingMetadata table1;
  private HousekeepingMetadata table2;
  private HousekeepingEntityServiceImpl beekeeperServiceImpl;

  @Mock
  private HousekeepingMetadataRepository housekeepingMetadataRepository;
  @Mock
  private Specification<HousekeepingMetadata> spec;
  @Mock
  private Pageable pageable;

  @BeforeEach
  public void createTables(){
    beekeeperServiceImpl = new HousekeepingEntityServiceImpl(housekeepingMetadataRepository);

    LocalDateTime CREATION_TIMESTAMP = LocalDateTime.now(ZoneId.of("UTC"));

    table1 = new HousekeepingMetadata.Builder()
        .path("s3://some/path/")
        .databaseName("aRandomDatabase")
        .tableName("aRandomTable")
        .partitionName("event_date=2020-01-01/event_hour=0/event_type=A")
        .housekeepingStatus(SCHEDULED)
        .creationTimestamp(CREATION_TIMESTAMP)
        .modifiedTimestamp(CREATION_TIMESTAMP)
        .cleanupDelay(Duration.parse("P3D"))
        .cleanupAttempts(0)
        .lifecycleType(EXPIRED.toString())
        .build();
    table2 = new HousekeepingMetadata.Builder()
        .path("s3://some/path2/")
        .databaseName("aRandomDatabase2")
        .tableName("aRandomTable2")
        .partitionName("event_date=2020-01-012/event_hour=2/event_type=B")
        .housekeepingStatus(DELETED)
        .creationTimestamp(CREATION_TIMESTAMP)
        .modifiedTimestamp(CREATION_TIMESTAMP)
        .cleanupDelay(Duration.parse("P3D"))
        .cleanupAttempts(0)
        .lifecycleType(EXPIRED.toString())
        .build();
  }

  @Test
  public void test(){

    List<HousekeepingMetadata> tables = new ArrayList<HousekeepingMetadata>();
    tables.add(table1);
    tables.add(table2);
    when(housekeepingMetadataRepository.findAll(spec, pageable)).thenReturn(new PageImpl<>(tables));
    Page<HousekeepingMetadata> result = beekeeperServiceImpl.returnAllTables(spec,pageable);

    assertThat(new PageImpl<>(tables),is(result));

  }

}
