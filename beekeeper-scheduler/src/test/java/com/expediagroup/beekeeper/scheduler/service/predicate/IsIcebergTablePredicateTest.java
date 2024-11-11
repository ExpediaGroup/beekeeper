/**
 * Copyright (C) 2019-2024 Expedia, Inc.
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
package com.expediagroup.beekeeper.scheduler.service.predicate;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class IsIcebergTablePredicateTest {

  private IsIcebergTablePredicate predicate = new IsIcebergTablePredicate();

  @Test
  void testIsIcebergTableByTableType() {
    Map<String, String> tableParameters = new HashMap<>();
    tableParameters.put("table_type", "ICEBERG");

    assertThat(predicate.test(tableParameters)).isTrue();
  }

  @Test
  void testIsNotIcebergTable() {
    Map<String, String> tableParameters = new HashMap<>();
    tableParameters.put("table_type", "EXTERNAL");

    assertThat(predicate.test(tableParameters)).isFalse();
  }

  @Test
  void testNullParameters() {
    assertThat(predicate.test(null)).isFalse();
  }

  @Test
  void testEmptyParameters() {
    Map<String, String> tableParameters = Collections.emptyMap();

    assertThat(predicate.test(tableParameters)).isFalse();
  }
}
