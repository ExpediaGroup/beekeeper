package com.expediagroup.beekeeper.api;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;

@ExtendWith(MockitoExtension.class)
public class ReturnTablesTest {

  private HousekeepingMetadataRepository housekeepingMetadataRepository;

  @Test
  public void test(){
    Optional<HousekeepingMetadata> housekeepingMetadataOptional = housekeepingMetadataRepository.findRecord("test","user_diaspora_v2");
  }


}
