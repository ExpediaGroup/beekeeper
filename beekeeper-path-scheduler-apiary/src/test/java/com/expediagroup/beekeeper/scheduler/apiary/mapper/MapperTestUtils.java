package com.expediagroup.beekeeper.scheduler.apiary.mapper;

import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageProperty;
import com.expedia.apiary.extensions.receiver.sqs.messaging.SqsMessageProperty;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.LifeCycleEventType;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class MapperTestUtils {

  private static final String RECEIPT_HANDLE = "receiptHandle";

  public static MessageEvent newMessageEvent(ListenerEvent event) {
    return new MessageEvent(event, MapperTestUtils.newMessageProperties());
  }

  public static Map<MessageProperty, String> newMessageProperties() {
    return Collections.singletonMap(SqsMessageProperty.SQS_MESSAGE_RECEIPT_HANDLE, RECEIPT_HANDLE);
  }

  public static void assertPath(
      HousekeepingPath path,
      String cleanupDelay,
      String tableName,
      String dbName,
      Integer cleanupAttempts,
      LocalDateTime creationTimestamp,
      String pathToCleanup,
      LifeCycleEventType lifeCycleEventType
  ) {
    LocalDateTime now = LocalDateTime.now();
    assertThat(path.getPath()).isEqualTo(pathToCleanup);
    assertThat(path.getTableName()).isEqualTo(tableName);
    assertThat(path.getDatabaseName()).isEqualTo(dbName);
    assertThat(path.getCleanupAttempts()).isEqualTo(cleanupAttempts);
    assertThat(path.getCleanupDelay()).isEqualTo(Duration.parse(cleanupDelay));
    assertThat(path.getLifecycleType()).isEqualToIgnoringCase(lifeCycleEventType.toString());
    assertThat(path.getModifiedTimestamp()).isNull();
    assertThat(path.getCreationTimestamp()).isBetween(creationTimestamp, now);
    assertThat(path.getCleanupTimestamp()).isEqualTo(path.getCreationTimestamp().plus(Duration.parse(cleanupDelay)));
  }

//
//  public static void setupMocks(
//      ListenerEvent event,
//      Boolean isUnreferenced,
//      Boolean isExpired,
//      Boolean delaysDefined,
//      String DATABASE,
//      String TABLE
//  ) {
//    Map<String,String> tableParams = new HashMap<>();
//
//    tableParams.put(UNREFERENCED.getTableParameterName(),isUnreferenced.toString().toLowerCase());
//    tableParams.put(EXPIRED.getTableParameterName(),isExpired.toString().toLowerCase());
//
//    if ( delaysDefined ) {
////            tableParams.put(CLEANUP_UNREFERENCED_DELAY_PROPERTY, CLEANUP_UNREFERENCED_DELAY);
//      tableParams.put(CLEANUP_EXPIRED_DELAY_PROPERTY, CLEANUP_EXPIRED_DELAY);
//    }
//
//    when(event.getTableParameters()).thenReturn(tableParams);
//    when(event.getDbName()).thenReturn(DATABASE);
//    when(event.getTableName()).thenReturn(TABLE);
//  }

}
