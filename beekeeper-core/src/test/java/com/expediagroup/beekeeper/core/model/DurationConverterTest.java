/**
 * Copyright (C) 2019 Expedia, Inc.
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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class DurationConverterTest {

  private final DurationConverter durationConverter = new DurationConverter();
  private final Duration threeDaysDuration = Duration.ofDays(3);
  private final Duration threeHoursDuration = Duration.ofHours(3);
  private final Duration threeMinutesDuration = Duration.ofMinutes(3);
  private final Duration threeSecondsDuration = Duration.ofSeconds(3);
  private final String threeDaysString = "P3D";
  private final String threeDaysInHoursString = "PT72H";
  private final String threeHoursString = "PT3H";
  private final String threeMinutesString = "PT3M";
  private final String threeSecondsString = "PT3S";

  @Test
  void convertToDatabaseColumn() {
    assertThat(durationConverter.convertToDatabaseColumn(threeDaysDuration)).isEqualTo(threeDaysInHoursString);
    assertThat(durationConverter.convertToDatabaseColumn(threeHoursDuration)).isEqualTo(threeHoursString);
    assertThat(durationConverter.convertToDatabaseColumn(threeMinutesDuration)).isEqualTo(threeMinutesString);
    assertThat(durationConverter.convertToDatabaseColumn(threeSecondsDuration)).isEqualTo(threeSecondsString);
    assertThat(durationConverter.convertToDatabaseColumn(null)).isNull();
  }

  @Test
  void convertToEntityAttribute() {
    assertThat(durationConverter.convertToEntityAttribute(threeDaysString)).isEqualTo(threeDaysDuration);
    assertThat(durationConverter.convertToEntityAttribute(threeHoursString)).isEqualTo(threeHoursDuration);
    assertThat(durationConverter.convertToEntityAttribute(threeMinutesString)).isEqualTo(threeMinutesDuration);
    assertThat(durationConverter.convertToEntityAttribute(threeSecondsString)).isEqualTo(threeSecondsDuration);
    assertThat(durationConverter.convertToEntityAttribute(null)).isNull();
  }
}
