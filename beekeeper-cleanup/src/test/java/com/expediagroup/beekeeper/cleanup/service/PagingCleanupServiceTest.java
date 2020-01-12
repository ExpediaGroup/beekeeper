package com.expediagroup.beekeeper.cleanup.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import com.expediagroup.beekeeper.cleanup.handler.GenericHandler;
import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;

@ExtendWith(MockitoExtension.class)
public class PagingCleanupServiceTest {

  private static final int PAGE_SIZE = 1;
  private final List<GenericHandler> handlersListMock = new ArrayList<>();
  @Mock private GenericHandler handler;
  @Mock private EntityHousekeepingPath housekeepingPath;
  private PagingCleanupService pagingCleanupService;

  @BeforeEach
  public void init() {
    handlersListMock.add(handler);
  }

  @Test
  public void typicalCleanup() {
    LocalDateTime nowDT = LocalDateTime.now();
    Instant nowInstant = nowDT.toInstant(ZoneOffset.UTC);

    when(handler.findRecordsToClean(nowDT, getPageRequest(0))).thenReturn(createMockPages(housekeepingPath, false));
    when(handler.findRecordsToClean(nowDT, getPageRequest(1))).thenReturn(createMockPages(null, true));

    pagingCleanupService = new PagingCleanupService(handlersListMock, PAGE_SIZE, false);
    pagingCleanupService.cleanUp(nowInstant);
    verify(handler).processPage(List.of(housekeepingPath), false);
  }

  @Test
  public void cleanupFails() {
    LocalDateTime nowDT = LocalDateTime.now();
    Instant nowInstant = nowDT.toInstant(ZoneOffset.UTC);

    when(handler.findRecordsToClean(nowDT, getPageRequest(0)))
        .thenThrow(RuntimeException.class);

    pagingCleanupService = new PagingCleanupService(handlersListMock, PAGE_SIZE, false);
    try {
      pagingCleanupService.cleanUp(nowInstant);
    } catch (Exception ex) {
      assertThat(ex).isInstanceOf(BeekeeperException.class);
      assertThat(ex.getMessage())
          .isEqualTo("Cleanup failed for instant " + nowInstant.toString());
    }
  }

  private Pageable getPageRequest(int requestNumber) {
    return PageRequest.of(requestNumber, PAGE_SIZE);
  }

  private Page<EntityHousekeepingPath> createMockPages(EntityHousekeepingPath housekeepingPath, boolean isEmpty) {
    if (isEmpty) {
      return new PageImpl<>(Collections.emptyList());
    }
    return new PageImpl<>(List.of(housekeepingPath));
  }
}
