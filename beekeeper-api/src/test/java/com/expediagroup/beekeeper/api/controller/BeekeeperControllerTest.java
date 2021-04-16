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
package com.expediagroup.beekeeper.api.controller;


import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import static com.expediagroup.beekeeper.api.util.DummyHousekeepingMetadataGenerator.generateDummyHousekeepingMetadata;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.expediagroup.beekeeper.api.service.HousekeepingMetadataServiceImpl;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;

@ExtendWith(SpringExtension.class)
@WebMvcTest(BeekeeperController.class)
public class BeekeeperControllerTest {

  @Autowired
  private MockMvc mockMvc;
  @Autowired
  private ObjectMapper objectMapper;

  @Mock
  private Specification<HousekeepingMetadata> spec;
  @Mock
  private Pageable pageable;

  @MockBean
  private HousekeepingMetadataServiceImpl housekeepingMetadataServiceImpl;

  @Test
  public void testGetAllWhenTablesValid() throws Exception {
    HousekeepingMetadata table1 = generateDummyHousekeepingMetadata("aRandomTable", "aRandomDatabase");
    HousekeepingMetadata table2 = generateDummyHousekeepingMetadata("aRandomTable2", "aRandomDatabase2");
    Page<HousekeepingMetadata> tables = new PageImpl<>(List.of(table1, table2));

    when(housekeepingMetadataServiceImpl.getAll(any(), any())).thenReturn(tables);

    mockMvc
        .perform(get("/api/v1/tables"))
        .andDo(MockMvcResultHandlers.print())
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
        .andExpect(content().json(objectMapper.writeValueAsString(tables)));
    verify(housekeepingMetadataServiceImpl, times(1)).getAll(any(), any());
    verifyNoMoreInteractions(housekeepingMetadataServiceImpl);
  }
}