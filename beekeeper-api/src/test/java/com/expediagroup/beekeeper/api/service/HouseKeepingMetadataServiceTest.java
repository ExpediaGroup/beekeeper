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

import static com.expediagroup.beekeeper.api.util.DummyHousekeepingMetadataGenerator.generateDummyHousekeepingMetadata;

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

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;

@ExtendWith(MockitoExtension.class)
public class HouseKeepingMetadataServiceTest {

  private HousekeepingMetadataService housekeepingMetadataService;

  @Mock
  private HousekeepingMetadataRepository housekeepingMetadataRepository;
  @Mock
  private Specification<HousekeepingMetadata> spec;
  @Mock
  private Pageable pageable;

  @BeforeEach
  public void beforeEach() {
    housekeepingMetadataService = new HousekeepingMetadataService(housekeepingMetadataRepository);
  }

  @Test
  public void testGetAllWhenTablesValid(){
    HousekeepingMetadata table1 = generateDummyHousekeepingMetadata("aRandomTable", "aRandomDatabase");
    HousekeepingMetadata table2 = generateDummyHousekeepingMetadata("aRandomTable2", "aRandomDatabase2");


    Page<HousekeepingMetadata> tables = new PageImpl<>(List.of(table1, table2));
    when(housekeepingMetadataRepository.findAll(spec, pageable)).thenReturn(tables);
    Page<HousekeepingMetadata> result = housekeepingMetadataService.getAll(spec, pageable);

    assertThat(tables).isEqualTo(result);
    verify(housekeepingMetadataRepository, times(1)).findAll(spec, pageable);
    verifyNoMoreInteractions(housekeepingMetadataRepository);
  }

}
