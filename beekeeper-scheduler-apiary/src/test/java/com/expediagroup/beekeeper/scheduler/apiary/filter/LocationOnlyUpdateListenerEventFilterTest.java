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
package com.expediagroup.beekeeper.scheduler.apiary.filter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.EventType;

@ExtendWith(MockitoExtension.class)
public class LocationOnlyUpdateListenerEventFilterTest {

  private static final String OLD_LOCATION = "old location";
  private static final String NEW_LOCATION = "new location";
  private final LocationOnlyUpdateListenerEventFilter locationOnlyUpdateListenerEventFilter = new LocationOnlyUpdateListenerEventFilter();
  private @Mock AlterPartitionEvent alterPartitionEvent;
  private @Mock AlterTableEvent alterTableEvent;
  private @Mock DropPartitionEvent dropPartitionEvent;
  private @Mock DropTableEvent dropTableEvent;

  @Test
  public void alterPartitionEventNotMetadataOnly() {
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    when(alterPartitionEvent.getOldPartitionLocation()).thenReturn(OLD_LOCATION);
    when(alterPartitionEvent.getPartitionLocation()).thenReturn(NEW_LOCATION);
    boolean filter = locationOnlyUpdateListenerEventFilter.isFiltered(alterPartitionEvent, UNREFERENCED);
    assertThat(filter).isFalse();
  }

  @Test
  public void alterTableEventNotMetadataOnly() {
    when(alterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);
    when(alterTableEvent.getOldTableLocation()).thenReturn(OLD_LOCATION);
    when(alterTableEvent.getTableLocation()).thenReturn(NEW_LOCATION);
    boolean filter = locationOnlyUpdateListenerEventFilter.isFiltered(alterTableEvent, UNREFERENCED);
    assertThat(filter).isFalse();
  }

  @Test
  public void alterTableEventMetadataOnly() {
    when(alterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);
    when(alterTableEvent.getOldTableLocation()).thenReturn(OLD_LOCATION);
    when(alterTableEvent.getTableLocation()).thenReturn(OLD_LOCATION);
    boolean filter = locationOnlyUpdateListenerEventFilter.isFiltered(alterTableEvent, UNREFERENCED);
    assertThat(filter).isTrue();
  }

  @Test
  public void alterPartitionEventMetadataOnly() {
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    when(alterPartitionEvent.getOldPartitionLocation()).thenReturn(OLD_LOCATION);
    when(alterPartitionEvent.getPartitionLocation()).thenReturn(OLD_LOCATION);
    boolean filter = locationOnlyUpdateListenerEventFilter.isFiltered(alterPartitionEvent, UNREFERENCED);
    assertThat(filter).isTrue();
  }

  @Test
  public void alterTableEventMetadataOnlyNullLocation() {
    when(alterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);
    when(alterTableEvent.getTableLocation()).thenReturn(null);
    boolean filter = locationOnlyUpdateListenerEventFilter.isFiltered(alterTableEvent, UNREFERENCED);
    assertThat(filter).isTrue();
  }

  @Test
  public void alterPartitionEventMetadataOnlyNullLocation() {
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    when(alterPartitionEvent.getPartitionLocation()).thenReturn(null);
    boolean filter = locationOnlyUpdateListenerEventFilter.isFiltered(alterPartitionEvent, UNREFERENCED);
    assertThat(filter).isTrue();
  }

  @Test
  public void alterTableEventMetadataOnlyNullOldLocation() {
    when(alterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);
    when(alterTableEvent.getOldTableLocation()).thenReturn(null);
    when(alterTableEvent.getTableLocation()).thenReturn(NEW_LOCATION);
    boolean filter = locationOnlyUpdateListenerEventFilter.isFiltered(alterTableEvent, UNREFERENCED);
    assertThat(filter).isTrue();
  }

  @Test
  public void alterPartitionEventMetadataOnlyNullOldLocation() {
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    when(alterPartitionEvent.getOldPartitionLocation()).thenReturn(null);
    when(alterPartitionEvent.getPartitionLocation()).thenReturn(NEW_LOCATION);
    boolean filter = locationOnlyUpdateListenerEventFilter.isFiltered(alterPartitionEvent, UNREFERENCED);
    assertThat(filter).isTrue();
  }

  @Test
  public void alterTableEventMetadataLocationTrailingSlashOldShouldFilter() {
    when(alterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);
    when(alterTableEvent.getOldTableLocation()).thenReturn("/foo/bar/");
    when(alterTableEvent.getTableLocation()).thenReturn("/foo/bar");
    boolean filter = locationOnlyUpdateListenerEventFilter.isFiltered(alterTableEvent, UNREFERENCED);
    assertThat(filter).isTrue();
  }

  @Test
  public void alterPartitionEventMetadataLocationTrailingSlashOldShouldFilter() {
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    when(alterPartitionEvent.getOldPartitionLocation()).thenReturn("/foo/bar/");
    when(alterPartitionEvent.getPartitionLocation()).thenReturn("/foo/bar");
    boolean filter = locationOnlyUpdateListenerEventFilter.isFiltered(alterPartitionEvent, UNREFERENCED);
    assertThat(filter).isTrue();
  }

  
  @Test
  public void alterTableEventMetadataLocationTrailingSlashNewShouldFilter() {
    when(alterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);
    when(alterTableEvent.getOldTableLocation()).thenReturn("/foo/bar");
    when(alterTableEvent.getTableLocation()).thenReturn("/foo/bar/");
    boolean filter = locationOnlyUpdateListenerEventFilter.isFiltered(alterTableEvent, UNREFERENCED);
    assertThat(filter).isTrue();
  }

  @Test
  public void alterPartitionEventMetadataLocationTrailingSlashNewShouldFilter() {
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    when(alterPartitionEvent.getOldPartitionLocation()).thenReturn("/foo/bar");
    when(alterPartitionEvent.getPartitionLocation()).thenReturn("/foo/bar/");
    boolean filter = locationOnlyUpdateListenerEventFilter.isFiltered(alterPartitionEvent, UNREFERENCED);
    assertThat(filter).isTrue();
  }

  @Test
  public void dropTableEvent() {
    when(dropTableEvent.getEventType()).thenReturn(EventType.DROP_TABLE);
    boolean filter = locationOnlyUpdateListenerEventFilter.isFiltered(dropTableEvent, UNREFERENCED);
    assertThat(filter).isFalse();
    verifyNoMoreInteractions(dropTableEvent);
  }

  @Test
  public void dropPartitionEvent() {
    when(dropPartitionEvent.getEventType()).thenReturn(EventType.DROP_PARTITION);
    boolean filter = locationOnlyUpdateListenerEventFilter.isFiltered(dropPartitionEvent, UNREFERENCED);
    assertThat(filter).isFalse();
    verifyNoMoreInteractions(dropPartitionEvent);
  }
}
