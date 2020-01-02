package com.expediagroup.beekeeper.scheduler.apiary.mapper;

import com.expedia.apiary.extensions.receiver.common.event.*;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.*;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class OrphanedPathMapperTest {

  private static final String DATABASE = "database";
  private static final String TABLE = "table";
  private static final String PATH = "some_path";
  private static final String OLD_PATH = "old_path";
  private static final Integer CLEANUP_ATTEMPTS = 0;
  private static final String CLEANUP_UNREFERENCED_DELAY = "P7D";
  private static final String CLEANUP_UNREFERENCED_DELAY_PROPERTY = "beekeeper.unreferenced.data.retention.period";
  private static final String DEFAULT_UNREFERENCED_CLEANUP_DELAY = "P3D";
  private static final LocalDateTime CREATION_TIMESTAMP = LocalDateTime.now();

  private @Mock ListenerEvent listenerEvent;
  private @Mock AlterTableEvent alterTableEvent;
  private @Mock AlterPartitionEvent alterPartitionEvent;
  private @Mock DropTableEvent dropTableEvent;
  private @Mock DropPartitionEvent dropPartitionEvent;

  @AfterEach
  public void afterEach() {
    reset(listenerEvent);
  }

  @Test
  public void shouldIgnoreAllEventsIfTableNotConfigured() {
    OrphanedPathMapper mapper = new OrphanedPathMapper(CLEANUP_UNREFERENCED_DELAY, CLEANUP_UNREFERENCED_DELAY_PROPERTY);

    for (EventType e : EventType.values()) {
      setupTableParams(listenerEvent, false, false);
      when(listenerEvent.getEventType()).thenReturn(e);
      List<HousekeepingPath> houseKeepingPaths = mapper.generateHouseKeepingPaths(listenerEvent);
      assertThat(houseKeepingPaths.size()).isEqualTo(0);
      reset(listenerEvent);
    }
  }

  @Test
  public void shouldIgnoreUnwatchedEvents() {
    OrphanedPathMapper mapper = new OrphanedPathMapper(CLEANUP_UNREFERENCED_DELAY, CLEANUP_UNREFERENCED_DELAY_PROPERTY);
    EventType[] ignoredEvents = { EventType.INSERT };

    for (EventType e : ignoredEvents) {
      setupTableParams(listenerEvent, true, true);
      when(listenerEvent.getEventType()).thenReturn(e);
      List<HousekeepingPath> houseKeepingPaths = mapper.generateHouseKeepingPaths(listenerEvent);
      assertThat(houseKeepingPaths.size()).isEqualTo(0);
      reset(listenerEvent);
    }
  }

  @Test
  public void shouldProperlyDefaultIfEventTimestampInvalid() {
    OrphanedPathMapper mapper = new OrphanedPathMapper(DEFAULT_UNREFERENCED_CLEANUP_DELAY, CLEANUP_UNREFERENCED_DELAY_PROPERTY);
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    when(alterPartitionEvent.getOldPartitionLocation()).thenReturn(OLD_PATH);
    when(alterPartitionEvent.getPartitionLocation()).thenReturn(PATH);
    when(alterPartitionEvent.getTableName()).thenReturn(TABLE);
    when(alterPartitionEvent.getDbName()).thenReturn(DATABASE);
    when(alterPartitionEvent.getTableParameters()).thenReturn(Map.of(
        UNREFERENCED.getTableParameterName(), "true",
        CLEANUP_UNREFERENCED_DELAY_PROPERTY, "INVALID_TIMESTAMP"
    ));

    List<HousekeepingPath> houseKeepingPaths = mapper.generateHouseKeepingPaths(alterPartitionEvent);
    assertThat(houseKeepingPaths.size()).isEqualTo(1);
    HousekeepingPath path = houseKeepingPaths.get(0);

    MapperTestUtils.assertPath(path, DEFAULT_UNREFERENCED_CLEANUP_DELAY, TABLE, DATABASE, CLEANUP_ATTEMPTS, CREATION_TIMESTAMP, OLD_PATH, UNREFERENCED);
  }

  @Test
  public void typicalMapAlterPartitionEvent() {
    OrphanedPathMapper mapper = new OrphanedPathMapper(CLEANUP_UNREFERENCED_DELAY, CLEANUP_UNREFERENCED_DELAY_PROPERTY);
    setupTableParams(alterPartitionEvent, true, true);
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    when(alterPartitionEvent.getOldPartitionLocation()).thenReturn(OLD_PATH);
    when(alterPartitionEvent.getPartitionLocation()).thenReturn(PATH);
    when(alterPartitionEvent.getTableName()).thenReturn(TABLE);
    when(alterPartitionEvent.getDbName()).thenReturn(DATABASE);

    List<HousekeepingPath> houseKeepingPaths = mapper.generateHouseKeepingPaths(alterPartitionEvent);
    assertThat(houseKeepingPaths.size()).isEqualTo(1);
    HousekeepingPath path = houseKeepingPaths.get(0);

    MapperTestUtils.assertPath(path, CLEANUP_UNREFERENCED_DELAY, TABLE, DATABASE, CLEANUP_ATTEMPTS, CREATION_TIMESTAMP, OLD_PATH, UNREFERENCED);
  }

  @Test
  public void nonMetadataMapAlterPartitionEvent() {
    OrphanedPathMapper mapper = new OrphanedPathMapper(CLEANUP_UNREFERENCED_DELAY, CLEANUP_UNREFERENCED_DELAY_PROPERTY);
    setupTableParams(alterPartitionEvent, true, true);
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    when(alterPartitionEvent.getOldPartitionLocation()).thenReturn(PATH);
    when(alterPartitionEvent.getPartitionLocation()).thenReturn(PATH);

    List<HousekeepingPath> houseKeepingPaths = mapper.generateHouseKeepingPaths(alterPartitionEvent);
    assertThat(houseKeepingPaths.size()).isEqualTo(0);
  }

  @Test
  public void typicalMapAlterTableEvent() {
    OrphanedPathMapper mapper = new OrphanedPathMapper(CLEANUP_UNREFERENCED_DELAY, CLEANUP_UNREFERENCED_DELAY_PROPERTY);
    setupTableParams(alterTableEvent, true, true);
    when(alterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);
    when(alterTableEvent.getDbName()).thenReturn(DATABASE);
    when(alterTableEvent.getTableName()).thenReturn(TABLE);
    when(alterTableEvent.getTableLocation()).thenReturn(PATH);
    when(alterTableEvent.getOldTableLocation()).thenReturn(OLD_PATH);

    List<HousekeepingPath> houseKeepingPaths = mapper.generateHouseKeepingPaths(alterTableEvent);
    assertThat(houseKeepingPaths.size()).isEqualTo(1);
    HousekeepingPath path = houseKeepingPaths.get(0);

    MapperTestUtils.assertPath(path, CLEANUP_UNREFERENCED_DELAY, TABLE, DATABASE, CLEANUP_ATTEMPTS, CREATION_TIMESTAMP, OLD_PATH, UNREFERENCED);
  }

  @Test
  public void nonMetadataMapAlterTableEvent() {
    OrphanedPathMapper mapper = new OrphanedPathMapper(CLEANUP_UNREFERENCED_DELAY, CLEANUP_UNREFERENCED_DELAY_PROPERTY);
    setupTableParams(alterTableEvent, true, true);
    when(alterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);
    when(alterTableEvent.getTableLocation()).thenReturn(PATH);
    when(alterTableEvent.getOldTableLocation()).thenReturn(PATH);

    List<HousekeepingPath> houseKeepingPaths = mapper.generateHouseKeepingPaths(alterTableEvent);
    assertThat(houseKeepingPaths.size()).isEqualTo(0);
  }

  @Test
  public void typicalMapDropTableEvent() {
    OrphanedPathMapper mapper = new OrphanedPathMapper(CLEANUP_UNREFERENCED_DELAY, CLEANUP_UNREFERENCED_DELAY_PROPERTY);
    setupTableParams(dropTableEvent, true, true);
    when(dropTableEvent.getEventType()).thenReturn(EventType.DROP_TABLE);
    when(dropTableEvent.getDbName()).thenReturn(DATABASE);
    when(dropTableEvent.getTableName()).thenReturn(TABLE);
    when(dropTableEvent.getTableLocation()).thenReturn(PATH);

    List<HousekeepingPath> houseKeepingPaths = mapper.generateHouseKeepingPaths(dropTableEvent);
    assertThat(houseKeepingPaths.size()).isEqualTo(1);
    HousekeepingPath path = houseKeepingPaths.get(0);

    MapperTestUtils.assertPath(path, CLEANUP_UNREFERENCED_DELAY, TABLE, DATABASE, CLEANUP_ATTEMPTS, CREATION_TIMESTAMP, PATH, UNREFERENCED);
  }

  @Test
  public void typicalAddPartitionEvent() {
    OrphanedPathMapper mapper = new OrphanedPathMapper(CLEANUP_UNREFERENCED_DELAY, CLEANUP_UNREFERENCED_DELAY_PROPERTY);

    setupTableParams(dropPartitionEvent, true, true);
    when(dropPartitionEvent.getEventType()).thenReturn(EventType.DROP_PARTITION);
    when(dropPartitionEvent.getTableName()).thenReturn(TABLE);
    when(dropPartitionEvent.getDbName()).thenReturn(DATABASE);
    when(dropPartitionEvent.getPartitionLocation()).thenReturn(PATH);

    List<HousekeepingPath> houseKeepingPaths = mapper.generateHouseKeepingPaths(dropPartitionEvent);
    assertThat(houseKeepingPaths.size()).isEqualTo(1);
    HousekeepingPath path = houseKeepingPaths.get(0);

    MapperTestUtils.assertPath(path, CLEANUP_UNREFERENCED_DELAY, TABLE, DATABASE, CLEANUP_ATTEMPTS, CREATION_TIMESTAMP, PATH, UNREFERENCED);
  }

  private void setupTableParams(ListenerEvent event, Boolean isUnreferenced, Boolean delaysDefined) {
    Map<String,String> tableParams = new HashMap<>();
    tableParams.put(UNREFERENCED.getTableParameterName(), isUnreferenced.toString().toLowerCase());
    if ( delaysDefined ) {
      tableParams.put(CLEANUP_UNREFERENCED_DELAY_PROPERTY, CLEANUP_UNREFERENCED_DELAY);
    }
    when(event.getTableParameters()).thenReturn(tableParams);
  }
}


