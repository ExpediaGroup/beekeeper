/**
 * Copyright (C) 2019-2023 Expedia, Inc.
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

import org.springdoc.api.annotations.ParameterObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import net.kaczmarzyk.spring.data.jpa.domain.EqualIgnoreCase;
import net.kaczmarzyk.spring.data.jpa.domain.GreaterThan;
import net.kaczmarzyk.spring.data.jpa.domain.LessThan;
import net.kaczmarzyk.spring.data.jpa.web.annotation.And;
import net.kaczmarzyk.spring.data.jpa.web.annotation.Spec;

import com.expediagroup.beekeeper.api.response.HousekeepingMetadataResponse;
import com.expediagroup.beekeeper.api.response.HousekeepingPathResponse;
import com.expediagroup.beekeeper.api.service.HousekeepingEntityService;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;

@RequestMapping("/api/v1")
@RestController
public class BeekeeperController {

  private final HousekeepingEntityService housekeepingEntityService;

  @Autowired
  public BeekeeperController(HousekeepingEntityService housekeepingEntityService) {
    this.housekeepingEntityService = housekeepingEntityService;
  }

  @RequestMapping(value = "/database/{databaseName}/table/{tableName}/metadata", method = RequestMethod.GET)
  @Parameter(name = "tableName", in = ParameterIn.PATH)
  @Parameter(name = "databaseName", in = ParameterIn.PATH)
  @Parameter(name = "path", in = ParameterIn.QUERY)
  @Parameter(name = "partition_name", in = ParameterIn.QUERY)
  @Parameter(name = "housekeeping_status", in = ParameterIn.QUERY)
  @Parameter(name = "lifecycle_type", in = ParameterIn.QUERY)
  @Parameter(name = "deleted_before", in = ParameterIn.QUERY)
  @Parameter(name = "deleted_after", in = ParameterIn.QUERY)
  @Parameter(name = "registered_before", in = ParameterIn.QUERY)
  @Parameter(name = "registered_after", in = ParameterIn.QUERY)
  public ResponseEntity<Page<HousekeepingMetadataResponse>> getAllMetadata(
      @PathVariable String databaseName,
      @PathVariable String tableName,
      @Parameter(hidden = true) @And(value = {
          @Spec(path = "tableName", pathVars = "tableName", spec = EqualIgnoreCase.class),
          @Spec(path = "databaseName", pathVars = "databaseName", spec = EqualIgnoreCase.class),
          @Spec(path = "path", params = "path", spec = EqualIgnoreCase.class),
          @Spec(path = "partitionName", params = "partition_name", spec = EqualIgnoreCase.class),
          @Spec(path = "housekeepingStatus", params = "housekeeping_status", spec = EqualIgnoreCase.class),
          @Spec(path = "lifecycleType", params = "lifecycle_type", spec = EqualIgnoreCase.class),
          @Spec(path = "cleanupTimestamp", params = "deleted_before", spec = LessThan.class),
          @Spec(path = "cleanupTimestamp", params = "deleted_after", spec = GreaterThan.class),
          @Spec(path = "creationTimestamp", params = "registered_before", spec = LessThan.class),
          @Spec(path = "creationTimestamp", params = "registered_after", spec = GreaterThan.class) }) Specification<HousekeepingMetadata> spec,
      @ParameterObject Pageable pageable) {
    return ResponseEntity.ok(housekeepingEntityService.getAllMetadata(spec, pageable));
  }


  @RequestMapping(value = "/database/{databaseName}/table/{tableName}/unreferenced-paths", method = RequestMethod.GET)
  @Parameter(name = "tableName", in = ParameterIn.PATH)
  @Parameter(name = "databaseName", in = ParameterIn.PATH)
  @Parameter(name = "path", in = ParameterIn.QUERY)
  @Parameter(name = "partition_name", in = ParameterIn.QUERY)
  @Parameter(name = "housekeeping_status", in = ParameterIn.QUERY)
  @Parameter(name = "lifecycle_type", in = ParameterIn.QUERY)
  @Parameter(name = "deleted_before", in = ParameterIn.QUERY)
  @Parameter(name = "deleted_after", in = ParameterIn.QUERY)
  @Parameter(name = "registered_before", in = ParameterIn.QUERY)
  @Parameter(name = "registered_after", in = ParameterIn.QUERY)
  public ResponseEntity<Page<HousekeepingPathResponse>> getAllPaths(
      @PathVariable String databaseName,
      @PathVariable String tableName,
      @Parameter(hidden = true) @And(value ={
          @Spec(path = "tableName", pathVars = "tableName", spec = EqualIgnoreCase.class),
          @Spec(path = "databaseName", pathVars = "databaseName", spec = EqualIgnoreCase.class),
          @Spec(path = "path", params = "path", spec = EqualIgnoreCase.class),
          @Spec(path = "partitionName", params = "partition_name", spec = EqualIgnoreCase.class),
          @Spec(path = "housekeepingStatus", params = "housekeeping_status", spec = EqualIgnoreCase.class),
          @Spec(path = "lifecycleType", params = "lifecycle_type", spec = EqualIgnoreCase.class),
          @Spec(path = "cleanupTimestamp", params = "deleted_before", spec = LessThan.class),
          @Spec(path = "cleanupTimestamp", params = "deleted_after", spec = GreaterThan.class),
          @Spec(path = "creationTimestamp", params = "registered_before", spec = LessThan.class),
          @Spec(path = "creationTimestamp", params = "registered_after", spec = GreaterThan.class) }) Specification<HousekeepingPath> spec,
      @ParameterObject Pageable pageable) {
    return ResponseEntity.ok(housekeepingEntityService.getAllPaths(spec, pageable));
  }

}
