package com.expediagroup.beekeeper.api.service;

import static com.expediagroup.beekeeper.api.response.HousekeepingMetadataResponse.convertToHouseKeepingMetadataResponsePage;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;

import com.expediagroup.beekeeper.api.response.HousekeepingMetadataResponse;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;

@Service
public class BeekeeperService {

  private final HousekeepingMetadataService housekeepingMetadataService;

  @Autowired
  public BeekeeperService(
      HousekeepingMetadataService housekeepingMetadataService) {this.housekeepingMetadataService = housekeepingMetadataService;}

  public Page<HousekeepingMetadataResponse> getAllTables(Specification<HousekeepingMetadata> spec, Pageable pageable) {
    return convertToHouseKeepingMetadataResponsePage(housekeepingMetadataService.getAll(spec, pageable).getContent());
  }
}
