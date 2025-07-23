/**
 * Copyright (C) 2019-2025 Expedia, Inc.
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
package com.expediagroup.beekeeper.api.error;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.PropertyReferenceException;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;

import com.expediagroup.beekeeper.core.model.HousekeepingPath;

public class BeekeeperExceptionHandlerTest {

  private final BeekeeperExceptionHandler exceptionHandler = new BeekeeperExceptionHandler();

  @Test
  public void handlePropertyReferenceException_ShouldReturnBadRequest() {
    String propertyName = "string";
    TypeInformation<?> typeInformation = ClassTypeInformation.from(HousekeepingPath.class);
    List<PropertyPath> baseProperty = Collections.emptyList();
    PropertyReferenceException exception = new PropertyReferenceException(propertyName, typeInformation, baseProperty);

    MockHttpServletRequest request = new MockHttpServletRequest();
    request.setRequestURI("/api/v1/database/testDb/table/testTable/unreferenced-paths");

    ResponseEntity<ErrorResponse> response = exceptionHandler.handlePropertyReferenceException(exception, request);

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

    ErrorResponse errorResponse = response.getBody();
    assertThat(errorResponse).isNotNull();
    assertThat(errorResponse.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST.value());
    assertThat(errorResponse.getError()).isEqualTo("Bad Request");
    assertThat(errorResponse.getMessage()).isEqualTo(exception.getMessage());
    assertThat(errorResponse.getPath()).isEqualTo("/api/v1/database/testDb/table/testTable/unreferenced-paths");
    assertThat(errorResponse.getTimestamp()).isNotNull();
  }
}
