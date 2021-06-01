package com.expediagroup.beekeeper.api.service;

import static com.expediagroup.beekeeper.api.response.HousekeepingMetadataResponse.convertToHouseKeepingMetadataResponsePage;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;

@Service
public class BeekeeperService implements HousekeepingEntityService<HousekeepingMetadata>{

  private final HousekeepingMetadataRepository housekeepingMetadataRepository;

  @Autowired
  public BeekeeperService(HousekeepingMetadataRepository housekeepingMetadataRepository) {
    this.housekeepingMetadataRepository = housekeepingMetadataRepository;
  }

  public Page<HousekeepingMetadata> getAll(Specification<HousekeepingMetadata> spec, Pageable pageable) {
    return housekeepingMetadataRepository.findAll(spec, pageable);
  }

  public Page<HousekeepingMetadata> findMetadataForDbAndTable(String dbName, String tableName, Specification<HousekeepingMetadata> spec, Pageable pageable) {
    return housekeepingMetadataRepository.findMetadataForDbAndTable(dbName, tableName, pageable);
  }
}
