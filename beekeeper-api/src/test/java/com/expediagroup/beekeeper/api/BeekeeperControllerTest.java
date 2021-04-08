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

import static com.expediagroup.beekeeper.api.DummyHousekeepingMetadataGenerator.generateDummyHousekeepingMetadata;
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
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;

@ExtendWith(MockitoExtension.class)
public class BeekeeperControllerTest {

  @Autowired
  private MockMvc mockMvc;
  @Autowired
  private ObjectMapper objectMapper;

  @Mock
  private HousekeepingMetadataRepository housekeepingMetadataRepository;
  @Mock
  private Specification<HousekeepingMetadata> spec;
  @Mock
  private Pageable pageable;

  private HousekeepingMetadataServiceImpl housekeepingMetadataServiceImpl;


  @BeforeEach
  public void beforeEach() {
    housekeepingMetadataServiceImpl = new HousekeepingMetadataServiceImpl(housekeepingMetadataRepository);
  }

  @Test
  public void testGetAllWhenTablesValid() throws Exception {
    HousekeepingMetadata table1 = generateDummyHousekeepingMetadata("aRandomTable", "aRandomDatabase");
    HousekeepingMetadata table2 = generateDummyHousekeepingMetadata("aRandomTable2", "aRandomDatabase2");
    Page<HousekeepingMetadata> tables = new PageImpl<>(List.of(table1, table2));

    when(housekeepingMetadataRepository.findAll(spec, pageable)).thenReturn(tables);

    mockMvc
        .perform(get(housekeepingMetadataServiceImpl.getAll(spec, pageable).toString()))
        .andDo(MockMvcResultHandlers.print())
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON))
        .andExpect(content().json(objectMapper.writeValueAsString(tables)));
  }
}
