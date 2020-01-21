package com.expediagroup.beekeeper.cleanup.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expediagroup.beekeeper.cleanup.path.aws.S3PathCleaner;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;

@ExtendWith(MockitoExtension.class)
public class UnreferencedHandlerTest {

  @Mock private S3PathCleaner s3PathCleaner;
  @InjectMocks private final UnreferencedHandler handler = new UnreferencedHandler(s3PathCleaner);
  @Mock private HousekeepingPathRepository housekeepingPathRepository;

  @Test
  public void verifyPathCleaner() {
    assertThat(handler.getPathCleaner()).isInstanceOf(S3PathCleaner.class);
  }

  @Test
  public void verifyLifecycle() {
    assertThat(handler.getLifecycleType()).isEqualTo(LifecycleEventType.UNREFERENCED);
  }

  @Test
  public void verifyHousekeepingPathFetch() {
    handler.findRecordsToClean(null, null);
    verify(housekeepingPathRepository).findRecordsForCleanupByModifiedTimestamp(null, null);
  }
}
