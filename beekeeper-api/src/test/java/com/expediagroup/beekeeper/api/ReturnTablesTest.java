package com.expediagroup.beekeeper.api;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;

@ExtendWith(MockitoExtension.class)
public class ReturnTablesTest {

  private HousekeepingMetadataRepository housekeepingMetadataRepository;
  private HousekeepingMetadata table1;
  private final BeekeeperServiceImpl beekeeperServiceImpl;

  public ReturnTablesTest(
      HousekeepingMetadataRepository housekeepingMetadataRepository,
      BeekeeperServiceImpl beekeeperServiceImpl) {this.housekeepingMetadataRepository = housekeepingMetadataRepository;
    this.beekeeperServiceImpl = beekeeperServiceImpl;
  }

  @BeforeEach
  public void createTables(){
    LocalDateTime CREATION_TIMESTAMP = LocalDateTime.now(ZoneId.of("UTC"));
    table1 = new HousekeepingMetadata.Builder()
        .path("s3://some/path/")
        .databaseName("aRandomDatabase")
        .tableName("aRandomTable")
        .partitionName("event_date=2020-01-01/event_hour=0/event_type=A")
        .housekeepingStatus(SCHEDULED)
        .creationTimestamp(CREATION_TIMESTAMP)
        .modifiedTimestamp(CREATION_TIMESTAMP)
        .cleanupDelay(Duration.parse("P3D"))
        .cleanupAttempts(0)
        .lifecycleType(EXPIRED.toString())
        .build();
  }

  @Test
  public void test(){
    beekeeperServiceImpl.saveTable(table1);
    List<HousekeepingMetadata> tables = beekeeperServiceImpl.returnAllTables();
    System.out.println("AAA tables:"+tables.size());
  }

}
