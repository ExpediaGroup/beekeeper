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

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;

@Builder
@Data
@Jacksonized
public class UnreferencedEventDetails {

  private static Logger log = LoggerFactory.getLogger(UnreferencedEventDetails.class);

  String cleanupDelay;
  String creationTimestamp;
  String modifiedTimestamp;
  String cleanupTimestamp;
  int cleanupAttempts;

  public static UnreferencedEventDetails fromEntity(HousekeepingPath path) {
    return UnreferencedEventDetails.builder()
        .cleanupDelay(path.getCleanupDelay().toString())
        .creationTimestamp(Objects.toString(path.getCreationTimestamp(), ""))
        .modifiedTimestamp(Objects.toString(path.getModifiedTimestamp(), ""))
        .cleanupTimestamp(Objects.toString(path.getCleanupTimestamp(), ""))
        .cleanupAttempts(path.getCleanupAttempts())
        .build();
  }

  public static String stringFromEntity(HousekeepingPath path){
    return fromEntity(path).toString();
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
