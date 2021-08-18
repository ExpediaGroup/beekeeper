/**
 * Copyright (C) 2019-2021 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.expediagroup.beekeeper.api.service;

import static com.expediagroup.beekeeper.api.response.MetadataResponseConverter.convertToHousekeepingMetadataResponsePage;
import static com.expediagroup.beekeeper.api.response.PathResponseConverter.convertToHousekeepingPathResponsePage;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;

import com.expediagroup.beekeeper.api.response.HousekeepingMetadataResponse;
import com.expediagroup.beekeeper.api.response.HousekeepingPathResponse;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;

@Service
public class HousekeepingEntityServiceImpl implements HousekeepingEntityService {

  private final HousekeepingMetadataRepository housekeepingMetadataRepository;
  private final HousekeepingPathRepository housekeepingPathRepository;

  @Autowired
  public HousekeepingEntityServiceImpl(HousekeepingMetadataRepository housekeepingMetadataRepository, HousekeepingPathRepository housekeepingPathRepository) {
    this.housekeepingMetadataRepository = housekeepingMetadataRepository;
    this.housekeepingPathRepository = housekeepingPathRepository;
  }

  public Page<HousekeepingMetadataResponse> getAllMetadata(
      Specification<HousekeepingMetadata> spec,
      Pageable pageable) {
    return convertToHousekeepingMetadataResponsePage(housekeepingMetadataRepository.findAll(spec, pageable));
  }

  public Page<HousekeepingPathResponse> getAllPaths(Specification<HousekeepingPath> spec, Pageable pageable) {
    return convertToHousekeepingPathResponsePage(housekeepingPathRepository.findAll(spec, pageable));
  }

}
