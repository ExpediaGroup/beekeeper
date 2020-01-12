/**
 * Copyright (C) 2019 Expedia, Inc.
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
package com.expediagroup.beekeeper.scheduler.filter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.event.AddPartitionEvent;

import com.expediagroup.beekeeper.scheduler.apiary.filter.FilterType;
import com.expediagroup.beekeeper.scheduler.apiary.filter.TableParameterListenerEventFilter;

@ExtendWith(MockitoExtension.class)
public class TableParameterListenerEventFilterTest {

  private static final String BEEKEEPER_TABLE_PARAMETER = "beekeeper.remove.unreferenced.data";
  private static final String NOT_BEEKEEPER_TABLE_PARAMETER = "incorrect key";
  private static final String BEEKEEPER_MANAGED = "true";
  private static final String NOT_BEEKEEPER_MANAGED = "false";

  @Mock
  private AddPartitionEvent alterPartitionEvent;

  private final TableParameterListenerEventFilter listenerEventFilter = new TableParameterListenerEventFilter();

  @Test
  public void checkTypeDeclaration() {
    assertThat(listenerEventFilter.getFilterType()).isEqualTo(FilterType.TABLE_PARAMETER);
  }

  @Test
  public void typicalFilterBeekeeperManaged() {
    when(alterPartitionEvent.getTableParameters())
        .thenReturn(Map.of(BEEKEEPER_TABLE_PARAMETER, BEEKEEPER_MANAGED));
    Boolean filter = listenerEventFilter.filter(alterPartitionEvent);
    assertThat(filter).isFalse();
  }

  @Test
  public void typicalFilterNotBeekeeperManaged() {
    when(alterPartitionEvent.getTableParameters())
        .thenReturn(Map.of(BEEKEEPER_TABLE_PARAMETER, NOT_BEEKEEPER_MANAGED));
    Boolean filter = listenerEventFilter.filter(alterPartitionEvent);
    assertThat(filter).isTrue();
  }

  @Test
  public void typicalFilterNotBeekeeperManagedEmptyMap() {
    when(alterPartitionEvent.getTableParameters())
        .thenReturn(Map.of());
    Boolean filter = listenerEventFilter.filter(alterPartitionEvent);
    assertThat(filter).isTrue();
  }

  @Test
  public void typicalFilterNotBeekeeperManagedNullMap() {
    when(alterPartitionEvent.getTableParameters())
        .thenReturn(null);
    Boolean filter = listenerEventFilter.filter(alterPartitionEvent);
    assertThat(filter).isTrue();
  }

  @Test
  public void typicalFilterBeekeeperManagedMapWithWrongKey() {
    when(alterPartitionEvent.getTableParameters())
        .thenReturn(Map.of(NOT_BEEKEEPER_TABLE_PARAMETER, BEEKEEPER_MANAGED));
    Boolean filter = listenerEventFilter.filter(alterPartitionEvent);
    assertThat(filter).isTrue();
  }
}
