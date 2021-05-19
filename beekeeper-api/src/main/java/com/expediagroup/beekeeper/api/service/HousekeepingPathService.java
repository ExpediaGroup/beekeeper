package com.expediagroup.beekeeper.api.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;

@Service
public class HousekeepingPathService implements HousekeepingEntityService<HousekeepingPath>{

  private final HousekeepingPathRepository housekeepingPathRepository;

  @Autowired
  public HousekeepingPathService(HousekeepingPathRepository housekeepingPathRepository) {
    this.housekeepingPathRepository = housekeepingPathRepository;
  }

  @Override
  public Page<HousekeepingPath> getAll(Specification<HousekeepingPath> spec, Pageable pageable) {
    return housekeepingPathRepository.findAll(spec, pageable);
  }
}
