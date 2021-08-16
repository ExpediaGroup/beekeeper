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
package com.expediagroup.beekeeper.api.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static com.expediagroup.beekeeper.api.response.MetadataResponseConverter.convertToHousekeepingMetadataResponsePage;
import static com.expediagroup.beekeeper.api.response.PathResponseConverter.convertToHousekeepingPathResponsePage;
import static com.expediagroup.beekeeper.api.util.DummyHousekeepingEntityGenerator.generateDummyHousekeepingMetadata;
import static com.expediagroup.beekeeper.api.util.DummyHousekeepingEntityGenerator.generateDummyHousekeepingPath;

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

import com.expediagroup.beekeeper.api.response.HousekeepingMetadataResponse;
import com.expediagroup.beekeeper.api.response.HousekeepingPathResponse;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;

@ExtendWith(MockitoExtension.class)
public class HousekeepingEntityServiceImplTest {

  private HousekeepingEntityServiceImpl housekeepingEntityServiceImpl;

  @Mock
  private HousekeepingMetadataRepository housekeepingMetadataRepository;
  @Mock
  private HousekeepingPathRepository housekeepingPathRepository;
  @Mock
  private Specification<HousekeepingMetadata> metadataSpec;
  @Mock
  private Specification<HousekeepingPath> pathsSpec;
  @Mock
  private Pageable pageable;

  @BeforeEach
  public void beforeEach() {
    housekeepingEntityServiceImpl = new HousekeepingEntityServiceImpl(housekeepingMetadataRepository, housekeepingPathRepository);
  }

  @Test
  public void testGetAllMetadata() {
    HousekeepingMetadata metadata1 = generateDummyHousekeepingMetadata("some_database", "some_table");
    HousekeepingMetadata metadata2 = generateDummyHousekeepingMetadata("some_database", "some_table");
    Page<HousekeepingMetadata> metadataPage = new PageImpl<>(List.of(metadata1, metadata2));
    Page<HousekeepingMetadataResponse> metadataResponsePage = convertToHousekeepingMetadataResponsePage(
        new PageImpl<>(List.of(metadata1, metadata2)));

    when(housekeepingMetadataRepository.findAll(metadataSpec, pageable)).thenReturn(metadataPage);
    Page<HousekeepingMetadataResponse> result = housekeepingEntityServiceImpl.getAllMetadata(metadataSpec, pageable);

    assertThat(result).isEqualTo(metadataResponsePage);
    verify(housekeepingMetadataRepository, times(1)).findAll(metadataSpec, pageable);
    verifyNoMoreInteractions(housekeepingMetadataRepository);
  }

  @Test
  public void testGetAllPaths() {
    HousekeepingPath path1 = generateDummyHousekeepingPath("some_database", "some_table");
    HousekeepingPath path2 = generateDummyHousekeepingPath("some_database", "some_table");
    Page<HousekeepingPath> pathsPage = new PageImpl<>(List.of(path1, path2));
    Page<HousekeepingPathResponse> pathsResponsePage = convertToHousekeepingPathResponsePage(
        new PageImpl<>(List.of(path1, path2)));

    when(housekeepingPathRepository.findAll(pathsSpec, pageable)).thenReturn(pathsPage);
    Page<HousekeepingPathResponse> result = housekeepingEntityServiceImpl.getAllPaths(pathsSpec, pageable);

    assertThat(result).isEqualTo(pathsResponsePage);
    verify(housekeepingPathRepository, times(1)).findAll(pathsSpec, pageable);
    verifyNoMoreInteractions(housekeepingPathRepository);
  }

}
