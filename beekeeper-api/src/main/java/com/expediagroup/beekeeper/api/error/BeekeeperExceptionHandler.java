/**
 * Copyright (C) 2019-2024 Expedia, Inc.
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

import javax.servlet.http.HttpServletRequest;

import org.springframework.data.mapping.PropertyReferenceException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.LocalDateTime;

@RestControllerAdvice
public class BeekeeperExceptionHandler {

  /**
   * Handles invalid sort parameters.
   *
   * @param exception  the exception is thrown when an invalid property is referenced
   * @param request the HTTP request
   * @return a ResponseEntity containing the error response
   */

  @ExceptionHandler(PropertyReferenceException.class)
  public ResponseEntity<ErrorResponse> handlePropertyReferenceException(
      PropertyReferenceException exception, HttpServletRequest request) {

    ErrorResponse errorResponse = ErrorResponse.builder()
        .timestamp(LocalDateTime.now().toString())
        .status(HttpStatus.BAD_REQUEST.value())
        .error(HttpStatus.BAD_REQUEST.getReasonPhrase())
        .message(exception.getMessage())
        .path(request.getRequestURI())
        .build();

    return new ResponseEntity<>(errorResponse, HttpStatus.BAD_REQUEST);
  }

}
