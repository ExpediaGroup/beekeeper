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
package com.expediagroup.beekeeper.api.controller;


import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import static com.expediagroup.beekeeper.api.response.HousekeepingMetadataResponse.convertToHouseKeepingMetadataResponsePage;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import net.kaczmarzyk.spring.data.jpa.domain.EqualIgnoreCase;
import net.kaczmarzyk.spring.data.jpa.domain.GreaterThan;
import net.kaczmarzyk.spring.data.jpa.domain.LessThan;
import net.kaczmarzyk.spring.data.jpa.web.annotation.And;
import net.kaczmarzyk.spring.data.jpa.web.annotation.Spec;

import com.expediagroup.beekeeper.api.response.HousekeepingMetadataResponse;
import com.expediagroup.beekeeper.api.service.BeekeeperService;
import com.expediagroup.beekeeper.api.service.HousekeepingMetadataService;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;

@RequestMapping("/api/v1")
@RestController
public class BeekeeperController {
  private final BeekeeperService beekeeperService;

  @Autowired
  public BeekeeperController(HousekeepingMetadataService housekeepingMetadataService,
      BeekeeperService beekeeperService) {
    this.beekeeperService = beekeeperService;
  }

  @GetMapping(path = "/tables", produces = APPLICATION_JSON_VALUE)
  public ResponseEntity<Page<HousekeepingMetadataResponse>> getAll(
      @And({
          @Spec(path = "tableName", params = "table_name", spec = EqualIgnoreCase.class),
          @Spec(path = "databaseName", params = "database_name", spec = EqualIgnoreCase.class),
          @Spec(path = "housekeepingStatus", params = "housekeeping_status", spec = EqualIgnoreCase.class),
          @Spec(path = "lifecycleType", params = "lifecycle_type", spec = EqualIgnoreCase.class),
          @Spec(path = "cleanupTimestamp", params = "deleted_before", spec = LessThan.class),
          @Spec(path = "cleanupTimestamp", params = "deleted_after", spec = GreaterThan.class),
          @Spec(path = "creationTimestamp", params = "registered_before", spec = LessThan.class),
          @Spec(path = "creationTimestamp", params = "registered_after", spec = GreaterThan.class)
      })
      Specification<HousekeepingMetadata> spec, Pageable pageable) {
    return ResponseEntity.ok(beekeeperService.getAllTables(spec, pageable));
  }

}
