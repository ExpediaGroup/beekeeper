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
package com.expediagroup.beekeeper.core.model;

import javax.persistence.AttributeConverter;
import javax.persistence.Converter;

@Converter(autoApply = true)
public class PeriodDurationConverter implements AttributeConverter<PeriodDuration, String> {

  @Override
  public String convertToDatabaseColumn(PeriodDuration period) {
    if (period != null) {
      return period.toString();
    } else {
      return null;
    }
  }

  @Override
  public PeriodDuration convertToEntityAttribute(String durationString) {
    if (durationString != null) {
      return PeriodDuration.parse(durationString);
    } else {
      return null;
    }
  }
}
