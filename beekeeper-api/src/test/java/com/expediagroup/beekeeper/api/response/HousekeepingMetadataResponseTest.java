/**
 * Copyright (C) 2019-2023 Expedia, Inc.
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
package com.expediagroup.beekeeper.api.response;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import static com.expediagroup.beekeeper.api.response.MetadataResponseConverter.convertToHousekeepingMetadataResponsePage;
import static com.expediagroup.beekeeper.api.util.DummyHousekeepingEntityGenerator.generateDummyHousekeepingMetadata;
import static com.expediagroup.beekeeper.api.util.DummyHousekeepingEntityGenerator.generateDummyHousekeepingMetadataResponse;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;

public class HousekeepingMetadataResponseTest {

  @Test
  public void testConvertToHouseKeepingMetadataResponsePage() {
    HousekeepingMetadata metadata1 = generateDummyHousekeepingMetadata("some_database1", "some_table1");
    HousekeepingMetadata metadata2 = generateDummyHousekeepingMetadata("some_database2", "some_table2");
    HousekeepingMetadataResponse metadataResponse1 = generateDummyHousekeepingMetadataResponse(metadata1);
    HousekeepingMetadataResponse metadataResponse2 = generateDummyHousekeepingMetadataResponse(metadata2);

    List<HousekeepingMetadata> housekeepingMetadataList = List.of(metadata1, metadata2);
    Page<HousekeepingMetadataResponse> metadataResponsePage = convertToHousekeepingMetadataResponsePage(
        new PageImpl<>(housekeepingMetadataList));

    List<HousekeepingMetadataResponse> metadataResponsePageList = metadataResponsePage.getContent();

    assertThat(metadataResponsePageList.get(0)).isEqualTo(metadataResponse1);
    assertThat(metadataResponsePageList.get(1)).isEqualTo(metadataResponse2);
    assertThat(metadataResponsePage.getTotalElements()).isEqualTo(2L);
    assertThat(metadataResponsePage.getTotalPages()).isEqualTo(1L);
    assertThat(metadataResponsePage.getPageable()).isEqualTo((new PageImpl<>(housekeepingMetadataList).getPageable()));
  }

  @Test
  public void testConvertToHouseKeepingMetadataResponsePageWithMultiplePages() {
    // Create a list of housekeeping metadata objects that is larger than the page size
    List<HousekeepingMetadata> housekeepingMetadataList = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      housekeepingMetadataList.add(generateDummyHousekeepingMetadata("some_database" + i, "some_table" + i));
    }

    // Create a page of housekeeping metadata objects
    Page<HousekeepingMetadata> metadataPage = new PageImpl<>(housekeepingMetadataList, PageRequest.of(0, 10), 50);

    // Convert the page of housekeeping metadata objects to a page of housekeeping metadata response objects
    Page<HousekeepingMetadataResponse> metadataResponsePage = convertToHousekeepingMetadataResponsePage(metadataPage);

    // Assert that the housekeeping metadata response page has the correct total elements and total pages
    assertThat(metadataResponsePage.getTotalElements()).isEqualTo(50L);
    assertThat(metadataResponsePage.getTotalPages()).isEqualTo(5L);
  }
}
