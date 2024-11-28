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
package com.expediagroup.beekeeper.core.predicate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IsIcebergTablePredicateTest {

  private IsIcebergTablePredicate predicate;

  @BeforeEach
  void setUp() {
    predicate = new IsIcebergTablePredicate();
  }

  @Test
  void testNullTableParameters() {
    assertFalse(predicate.test(null));
  }

  @Test
  void testEmptyTableParameters() {
    Map<String, String> tableParameters = new HashMap<>();
    assertFalse(predicate.test(tableParameters));
  }

  @Test
  void testNoMetadataLocationOrTableType() {
    Map<String, String> tableParameters = Map.of("some_key", "some_value");
    assertFalse(predicate.test(tableParameters));
  }

  @Test
  void testHasMetadataLocation() {
    Map<String, String> tableParameters = Map.of("metadata_location", "some/location/path");
    assertTrue(predicate.test(tableParameters));
  }

  @Test
  void testHasIcebergTableType() {
    Map<String, String> tableParameters = Map.of("table_type", "ICEBERG");
    assertTrue(predicate.test(tableParameters));
  }

  @Test
  void testBothMetadataLocationAndTableType() {
    Map<String, String> tableParameters = Map.of(
        "metadata_location", "some/location/path",
        "table_type", "iceberg");
    assertTrue(predicate.test(tableParameters));
  }

  @Test
  void testCaseInsensitiveIcebergType() {
    Map<String, String> tableParameters = Map.of("table_type", "IcEbErG");
    assertTrue(predicate.test(tableParameters));
  }

  @Test
  void testWhitespaceInMetadataLocation() {
    Map<String, String> tableParameters = Map.of("metadata_location", "   ");
    assertFalse(predicate.test(tableParameters));
  }

  @Test
  void testIrrelevantTableType() {
    Map<String, String> tableParameters = Map.of("table_type", "hive");
    assertFalse(predicate.test(tableParameters));
  }
}
