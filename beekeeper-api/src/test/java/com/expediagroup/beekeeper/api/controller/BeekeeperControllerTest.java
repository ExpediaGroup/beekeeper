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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import static com.expediagroup.beekeeper.api.response.MetadataResponseConverter.convertToHousekeepingMetadataResponsePage;
import static com.expediagroup.beekeeper.api.response.PathResponseConverter.convertToHousekeepingPathResponsePage;
import static com.expediagroup.beekeeper.api.util.DummyHousekeepingEntityGenerator.generateDummyHousekeepingMetadata;
import static com.expediagroup.beekeeper.api.util.DummyHousekeepingEntityGenerator.generateDummyHousekeepingPath;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.expediagroup.beekeeper.api.TestApplication;
import com.expediagroup.beekeeper.api.response.HousekeepingMetadataResponse;
import com.expediagroup.beekeeper.api.response.HousekeepingPathResponse;
import com.expediagroup.beekeeper.api.service.HousekeepingEntityServiceImpl;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;

@WebMvcTest(BeekeeperController.class)
@ContextConfiguration(classes = TestApplication.class)
public class BeekeeperControllerTest {

  @Autowired
  private MockMvc mockMvc;
  @Autowired
  private ObjectMapper objectMapper;

  @MockBean
  private HousekeepingEntityServiceImpl housekeepingEntityServiceImpl;

  @Test
  public void testGetAllMetadataWhenValidInput() throws Exception {
    HousekeepingMetadata metadata = generateDummyHousekeepingMetadata("some_database", "some_table");
    Page<HousekeepingMetadataResponse> metadataResponsePage = convertToHousekeepingMetadataResponsePage(
        new PageImpl<>(List.of(metadata)));

    when(housekeepingEntityServiceImpl.getAllMetadata(any(), any())).thenReturn(metadataResponsePage);

    mockMvc
        .perform(get("/api/v1/database/some_database/table/some_table/metadata"))
        .andDo(MockMvcResultHandlers.print())
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
        .andExpect(content().json(objectMapper.writeValueAsString(metadataResponsePage)));
    verify(housekeepingEntityServiceImpl, times(1)).getAllMetadata(any(), any());
    verifyNoMoreInteractions(housekeepingEntityServiceImpl);
  }

  @Test
  public void testGetAllPathsWhenValidInput() throws Exception {
    HousekeepingPath path = generateDummyHousekeepingPath("some_database", "some_table");
    Page<HousekeepingPathResponse> pathsResponsePage = convertToHousekeepingPathResponsePage(
        new PageImpl<>(List.of(path)));

    when(housekeepingEntityServiceImpl.getAllPaths(any(), any())).thenReturn(pathsResponsePage);

    mockMvc
        .perform(get("/api/v1/database/some_database/table/some_table/unreferencedPaths"))
        .andDo(MockMvcResultHandlers.print())
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
        .andExpect(content().json(objectMapper.writeValueAsString(pathsResponsePage)));
    verify(housekeepingEntityServiceImpl, times(1)).getAllPaths(any(), any());
    verifyNoMoreInteractions(housekeepingEntityServiceImpl);
  }

  @Test
  public void testControllerWhenWrongUrl() throws Exception {
    mockMvc
        .perform(get("/api/v1/database/some_database/table/some_table/metadataa"))
        .andDo(MockMvcResultHandlers.print())
        .andExpect(status().isNotFound());
  }

  @Test
  public void testPagingWhenValidInput() throws Exception {
    int pageNumber = 5;
    int pageSize = 10;
    HousekeepingMetadata metadata1 = generateDummyHousekeepingMetadata("some_database", "some_table");
    HousekeepingMetadata metadata2 = generateDummyHousekeepingMetadata("some_database", "some_table");
    Page<HousekeepingMetadataResponse> metadataResponsePage = convertToHousekeepingMetadataResponsePage(
        new PageImpl<>(List.of(metadata1, metadata2)));

    when(housekeepingEntityServiceImpl.getAllMetadata(any(), eq(PageRequest.of(pageNumber, pageSize))))
        .thenReturn(metadataResponsePage);

    mockMvc
        .perform(get("/api/v1/database/some_database/table/some_table/metadata")
            .param("page", String.valueOf(pageNumber))
            .param("size", String.valueOf(pageSize)))
        .andDo(MockMvcResultHandlers.print())
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
        .andExpect(content().json(objectMapper.writeValueAsString(metadataResponsePage)));
    verify(housekeepingEntityServiceImpl, times(1)).getAllMetadata(any(), any());
    verifyNoMoreInteractions(housekeepingEntityServiceImpl);
  }

}
