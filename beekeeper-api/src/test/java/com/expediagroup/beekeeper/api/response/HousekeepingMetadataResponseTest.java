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
package com.expediagroup.beekeeper.api.response;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import static com.expediagroup.beekeeper.api.response.MetadataResponseConverter.convertToHousekeepingMetadataResponsePage;
import static com.expediagroup.beekeeper.api.util.DummyHousekeepingMetadataGenerator.generateDummyHousekeepingMetadata;
import static com.expediagroup.beekeeper.api.util.DummyHousekeepingMetadataGenerator.generateDummyHousekeepingMetadataResponse;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;

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
    assertThat(metadataResponsePage.getPageable()).isEqualTo((new PageImpl<>(housekeepingMetadataList).getPageable()));
  }

}
