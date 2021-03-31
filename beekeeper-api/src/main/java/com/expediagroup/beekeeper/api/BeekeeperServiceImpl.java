package com.expediagroup.beekeeper.api;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;

@Service
public class BeekeeperServiceImpl {

  private HousekeepingMetadataRepository housekeepingMetadataRepository;

  // Method for the GET tables/ endpoint
  public List<HousekeepingMetadata> returnAllTables() {
    return housekeepingMetadataRepository.findAll();
  }
}
