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
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;

@ExtendWith(MockitoExtension.class)
public class BeekeeperControllerTest {

  @Autowired
  private MockMvc mockMvc;
  @Autowired
  private ObjectMapper objectMapper;

  @Mock
  private Specification<HousekeepingMetadata> spec;
  @Mock
  private Pageable pageable;
  @Mock
  private HousekeepingMetadataService housekeepingMetadataService;

  private HousekeepingMetadata table1;
  private HousekeepingMetadata table2;

  @BeforeEach
  public void createTables(){

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
  public void getAllTables() throws Exception {
    List<HousekeepingMetadata> tablesList = new ArrayList<HousekeepingMetadata>();
    tablesList.add(table1);
    tablesList.add(table2);
    Page<HousekeepingMetadata> tables = new PageImpl<>(tablesList);
    System.out.println(tables.getContent());
    when(housekeepingMetadataService.returnAllTables(spec, pageable)).thenReturn(tables);

    mockMvc
        .perform(get(housekeepingMetadataService.returnAllTables(spec, pageable).toString()))
        .andDo(MockMvcResultHandlers.print())
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON))
        .andExpect(content().json(objectMapper.writeValueAsString(tables)));
  }
}
