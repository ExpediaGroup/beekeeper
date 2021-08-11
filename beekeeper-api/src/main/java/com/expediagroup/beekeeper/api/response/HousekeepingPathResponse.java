package com.expediagroup.beekeeper.api.response;

import java.time.Duration;
import java.time.LocalDateTime;

import org.hibernate.annotations.UpdateTimestamp;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;

import com.expediagroup.beekeeper.core.model.HousekeepingStatus;

@Value
@Builder
public class HousekeepingPathResponse {

  String path;

  String databaseName;

  String tableName;

  HousekeepingStatus housekeepingStatus;

  @EqualsAndHashCode.Exclude
  LocalDateTime creationTimestamp;

  @EqualsAndHashCode.Exclude
  @UpdateTimestamp
  LocalDateTime modifiedTimestamp;

  LocalDateTime cleanupTimestamp;

  Duration cleanupDelay;

  int cleanupAttempts;

  String lifecycleType;

}