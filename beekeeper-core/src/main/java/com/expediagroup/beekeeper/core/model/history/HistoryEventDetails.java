package com.expediagroup.beekeeper.core.model.history;

import java.time.LocalDateTime;

import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Builder
@Data
@Jacksonized
public class HistoryEventDetails {

  Long id;
  LocalDateTime eventTimestamp;
  String databaseName;
  String tableName;
  String lifecycleType;
  String housekeepingStatus;
  String eventDetails;
}
