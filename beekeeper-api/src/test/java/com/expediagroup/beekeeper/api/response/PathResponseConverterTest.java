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

import static com.expediagroup.beekeeper.api.response.PathResponseConverter.convertToHousekeepingPathResponsePage;
import static com.expediagroup.beekeeper.api.util.DummyHousekeepingEntityGenerator.generateDummyHousekeepingPath;
import static com.expediagroup.beekeeper.api.util.DummyHousekeepingEntityGenerator.generateDummyHousekeepingPathResponse;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;

import com.expediagroup.beekeeper.core.model.HousekeepingPath;

public class PathResponseConverterTest {

  @Test
  public void testConvertToHousekeepingPathResponsePage() {
    HousekeepingPath housekeepingPath1 = generateDummyHousekeepingPath("some_database1", "some_table1");
    HousekeepingPath housekeepingPath2 = generateDummyHousekeepingPath("some_database2", "some_table2");
    HousekeepingPathResponse housekeepingPathResponse1 = generateDummyHousekeepingPathResponse(housekeepingPath1);
    HousekeepingPathResponse housekeepingPathResponse2 = generateDummyHousekeepingPathResponse(housekeepingPath2);

    List<HousekeepingPath> housekeepingPathList = List.of(housekeepingPath1, housekeepingPath2);
    Page<HousekeepingPathResponse> housekeepingPathResponsePage = convertToHousekeepingPathResponsePage(
        new PageImpl<>(housekeepingPathList));

    List<HousekeepingPathResponse> housekeepingPathResponsePageList = housekeepingPathResponsePage.getContent();

    assertThat(housekeepingPathResponsePageList.get(0)).isEqualTo(housekeepingPathResponse1);
    assertThat(housekeepingPathResponsePageList.get(1)).isEqualTo(housekeepingPathResponse2);
    assertThat(housekeepingPathResponsePage.getTotalElements()).isEqualTo(2L);
    assertThat(housekeepingPathResponsePage.getTotalPages()).isEqualTo(1L);
    assertThat(housekeepingPathResponsePage.getPageable()).isEqualTo((new PageImpl<>(housekeepingPathList).getPageable()));
  }
}