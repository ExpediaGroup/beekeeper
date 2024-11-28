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
package com.expediagroup.beekeeper.cleanup.validation;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.expediagroup.beekeeper.cleanup.metadata.CleanerClient;
import com.expediagroup.beekeeper.cleanup.metadata.CleanerClientFactory;
import com.expediagroup.beekeeper.core.error.BeekeeperIcebergException;

public class IcebergValidatorTest {

  private CleanerClientFactory cleanerClientFactory;
  private CleanerClient cleanerClient;
  private IcebergValidator icebergValidator;

  @Before
  public void setUp() throws Exception {
    cleanerClientFactory = mock(CleanerClientFactory.class);
    cleanerClient = mock(CleanerClient.class);
    when(cleanerClientFactory.newInstance()).thenReturn(cleanerClient);
    icebergValidator = new IcebergValidator(cleanerClientFactory);
  }

  @Test(expected = BeekeeperIcebergException.class)
  public void shouldThrowExceptionWhenTableTypeIsIceberg() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("table_type", "ICEBERG");

    when(cleanerClient.getTableProperties("db", "table")).thenReturn(properties);

    icebergValidator.throwExceptionIfIceberg("db", "table");
    verify(cleanerClientFactory).newInstance();
    verify(cleanerClient).close();
  }

  @Test(expected = BeekeeperIcebergException.class)
  public void shouldThrowExceptionWhenMetadataIsIceberg() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("metadata_location", "s3://db/table/metadata/0000.json");

    when(cleanerClient.getTableProperties("db", "table")).thenReturn(properties);

    icebergValidator.throwExceptionIfIceberg("db", "table");
  }

  @Test
  public void shouldNotThrowExceptionForNonIcebergTable() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("table_type", "HIVE_TABLE");

    when(cleanerClient.getTableProperties("db", "table")).thenReturn(properties);

    icebergValidator.throwExceptionIfIceberg("db", "table");
    verify(cleanerClientFactory).newInstance();
    verify(cleanerClient).close();
  }

  @Test
  public void shouldThrowExceptionWhenOutputFormatIsNull() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("table_type", null);
    properties.put("metadata_location", null);

    when(cleanerClient.getTableProperties("db", "table")).thenReturn(properties);

    assertThatThrownBy(() -> icebergValidator.throwExceptionIfIceberg("db", "table")).isInstanceOf(
        BeekeeperIcebergException.class);
  }
}
