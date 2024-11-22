package com.expediagroup.beekeeper.core.model.history;

import java.time.LocalDateTime;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.PeriodDuration;

@Builder
@Data
@Jacksonized
public class ExpiredEventDetails {

  private static Logger log = LoggerFactory.getLogger(ExpiredEventDetails.class);

  String partitionName;
  String cleanupDelay;
  String creationTimestamp;
  String modifiedTimestamp;
  String cleanupTimestamp;
  int cleanupAttempts;

  public static ExpiredEventDetails fromEntity(HousekeepingMetadata metadata){
    return ExpiredEventDetails.builder()
        .partitionName(metadata.getPartitionName())
        .cleanupDelay(metadata.getCleanupDelay().toString())
        .creationTimestamp(Objects.toString(metadata.getCreationTimestamp(), ""))
        .modifiedTimestamp(Objects.toString(metadata.getModifiedTimestamp(), ""))
        .cleanupTimestamp(Objects.toString(metadata.getCleanupTimestamp(), ""))
        .cleanupAttempts(metadata.getCleanupAttempts())
        .build();
  }

  public static String stringFromEntity(HousekeepingMetadata metadata){
    return fromEntity(metadata).toString();
  }

  @Override
  public String toString(){
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      log.warn("Error encountered writing object as string", e);
    }
    return "{}";
  }
}
