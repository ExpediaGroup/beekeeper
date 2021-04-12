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


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import net.kaczmarzyk.spring.data.jpa.domain.EqualIgnoreCase;
import net.kaczmarzyk.spring.data.jpa.domain.GreaterThan;
import net.kaczmarzyk.spring.data.jpa.domain.LessThan;
import net.kaczmarzyk.spring.data.jpa.web.annotation.And;
import net.kaczmarzyk.spring.data.jpa.web.annotation.Spec;

import com.expediagroup.beekeeper.api.service.HousekeepingMetadataServiceImpl;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;

@RequestMapping("/api/v1")
@RestController
public class BeekeeperController {

  private final HousekeepingMetadataServiceImpl housekeepingMetadataServiceImpl;

  @Autowired
  public BeekeeperController(HousekeepingMetadataServiceImpl housekeepingMetadataServiceImpl) {
    this.housekeepingMetadataServiceImpl = housekeepingMetadataServiceImpl;
  }

  @GetMapping(path = "/tables", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Page<HousekeepingMetadata>> getAll(
      @And({
          @Spec(path = "tableName", spec = EqualIgnoreCase.class),
          @Spec(path = "databaseName", spec = EqualIgnoreCase.class),
          @Spec(path = "status", spec = EqualIgnoreCase.class),
          @Spec(path = "deletedBefore", spec = LessThan.class),
          @Spec(path = "deletedAfter", spec = GreaterThan.class)
      })
      Specification<HousekeepingMetadata> spec, Pageable pageable) {
      return ResponseEntity.ok(housekeepingMetadataServiceImpl.getAll(spec, pageable));
  }

}
