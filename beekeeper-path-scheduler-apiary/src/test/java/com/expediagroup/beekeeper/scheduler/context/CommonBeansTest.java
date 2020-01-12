package com.expediagroup.beekeeper.scheduler.context;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.EnumMap;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.messaging.MessageReader;
import com.expedia.apiary.extensions.receiver.sqs.messaging.SqsMessageReader;

import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.scheduler.apiary.context.CommonBeans;
import com.expediagroup.beekeeper.scheduler.apiary.filter.FilterType;
import com.expediagroup.beekeeper.scheduler.apiary.filter.ListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.BeekeeperEventReader;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.RetryingMessageReader;
import com.expediagroup.beekeeper.scheduler.service.SchedulerService;

@ExtendWith(MockitoExtension.class)
public class CommonBeansTest {

  private static final String AWS_S3_ENDPOINT_PROPERTY = "aws.s3.endpoint";
  private static final String AWS_REGION_PROPERTY = "aws.region";
  private static final String REGION = "us-west-2";
  private static final String AWS_ENDPOINT = String.join(".", "s3", REGION, "amazonaws.com");
  private static final String ENDPOINT = "endpoint";
  private static final String BUCKET = "bucket";
  private static final String KEY = "key";
  private final CommonBeans commonBeans = new CommonBeans();
  @Mock private MessageReader messageReader;

  @AfterAll
  static void teardown() {
    System.clearProperty(AWS_REGION_PROPERTY);
    System.clearProperty(AWS_S3_ENDPOINT_PROPERTY);
  }

  @BeforeEach
  void setUp() {
    System.setProperty(AWS_REGION_PROPERTY, REGION);
    System.setProperty(AWS_S3_ENDPOINT_PROPERTY, ENDPOINT);
  }

  @Test
  public void validateSchedulerServiceMap() {
    EnumMap<LifecycleEventType, SchedulerService> scheduleMap = commonBeans.schedulerServiceMap(Collections.EMPTY_LIST);
    assertThat(scheduleMap).isInstanceOf(EnumMap.class);
  }

  @Test
  public void validateFilterTypeMap() {
    EnumMap<FilterType, ListenerEventFilter> filterMap = commonBeans.filterTypeMap(Collections.EMPTY_LIST);
    assertThat(filterMap).isInstanceOf(EnumMap.class);
  }

  @Test
  public void validateMessageReader() {
    MessageReader reader = commonBeans.messageReader("some_path");
    assertThat(reader).isInstanceOf(SqsMessageReader.class);
  }

  @Test
  public void validateRetryingMessageReader() {
    MessageReader reader = commonBeans.retryingMessageReader(messageReader);
    assertThat(reader).isInstanceOf(RetryingMessageReader.class);
  }

  @Test
  public void validatePathEventReader() {
    BeekeeperEventReader reader = commonBeans.pathEventReader(messageReader, Collections.emptyList());
    assertThat(reader).isInstanceOf(BeekeeperEventReader.class);
  }
}
