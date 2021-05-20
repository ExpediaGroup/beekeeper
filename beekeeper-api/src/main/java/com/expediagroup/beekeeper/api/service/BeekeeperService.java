package com.expediagroup.beekeeper.api.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;

@Service
public class BeekeeperService {

  private final HousekeepingMetadataService housekeepingMetadataService;

  @Autowired
  public BeekeeperService(
      HousekeepingMetadataService housekeepingMetadataService) {this.housekeepingMetadataService = housekeepingMetadataService;}

  public Page<HousekeepingMetadata> getAllTables(Specification<HousekeepingMetadata> spec, Pageable pageable) {
    return housekeepingMetadataService.getAll(spec, pageable);
  }
}
