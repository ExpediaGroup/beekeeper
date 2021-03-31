package com.expediagroup.beekeeper.api;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;

@Service
public class BeekeeperServiceImpl {

  private HousekeepingMetadataRepository housekeepingMetadataRepository;

  @Autowired
  public BeekeeperServiceImpl(HousekeepingMetadataRepository housekeepingMetadataRepository) {
    this.housekeepingMetadataRepository = housekeepingMetadataRepository;
  }

  public void saveTable(HousekeepingMetadata table){
    housekeepingMetadataRepository.save(table);
    System.out.println("count:"+housekeepingMetadataRepository.count());
  }

  // Method for the GET tables/ endpoint
  public List<HousekeepingMetadata> returnAllTables() {
    return housekeepingMetadataRepository.findAll();
  }
}
