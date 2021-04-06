package com.expediagroup.beekeeper.api;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Repository;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;

@Repository
  public interface BeekeeperRepository extends PagingAndSortingRepository<HousekeepingMetadata, HousekeepingMetadata>,
    JpaSpecificationExecutor<HousekeepingMetadata> {

  }

