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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Period;

import org.junit.jupiter.api.Test;

class PeriodDurationConverterTest {

  private final PeriodDurationConverter periodDurationConverter = new PeriodDurationConverter();
  private final PeriodDuration monthsAndDaysDuration = PeriodDuration.of(Period.ofMonths(3), Duration.ofDays(3));
  private final PeriodDuration threeYearsDuration = PeriodDuration.of(Period.ofYears(3));
  private final PeriodDuration threeMonthsDuration = PeriodDuration.of(Period.ofMonths(3));
  private final PeriodDuration threeDaysPeriod = PeriodDuration.of(Period.ofDays(3));
  private final PeriodDuration threeDaysDuration = PeriodDuration.of(Duration.ofDays(3));
  private final PeriodDuration threeHoursDuration = PeriodDuration.of(Duration.ofHours(3));
  private final PeriodDuration threeMinutesDuration = PeriodDuration.of(Duration.ofMinutes(3));
  private final PeriodDuration threeSecondsDuration = PeriodDuration.of(Duration.ofSeconds(3));
  private final String monthsAndDaysString = "P3MT72H";
  private final String threeMonthsString = "P3M";
  private final String threeYearsString = "P3Y";
  private final String threeDaysString = "P3D";
  private final String threeDaysInHoursString = "PT72H";
  private final String threeHoursString = "PT3H";
  private final String threeMinutesString = "PT3M";
  private final String threeSecondsString = "PT3S";

  @Test
  void convertToDatabaseColumn() {
    assertThat(periodDurationConverter.convertToDatabaseColumn(monthsAndDaysDuration)).isEqualTo(monthsAndDaysString);
    assertThat(periodDurationConverter.convertToDatabaseColumn(threeYearsDuration)).isEqualTo(threeYearsString);
    assertThat(periodDurationConverter.convertToDatabaseColumn(threeMonthsDuration)).isEqualTo(threeMonthsString);
    assertThat(periodDurationConverter.convertToDatabaseColumn(threeDaysDuration)).isEqualTo(threeDaysInHoursString);
    assertThat(periodDurationConverter.convertToDatabaseColumn(threeDaysPeriod)).isEqualTo(threeDaysString);
    assertThat(periodDurationConverter.convertToDatabaseColumn(threeHoursDuration)).isEqualTo(threeHoursString);
    assertThat(periodDurationConverter.convertToDatabaseColumn(threeMinutesDuration)).isEqualTo(threeMinutesString);
    assertThat(periodDurationConverter.convertToDatabaseColumn(threeSecondsDuration)).isEqualTo(threeSecondsString);
    assertThat(periodDurationConverter.convertToDatabaseColumn(null)).isNull();
  }

  @Test
  void convertToEntityAttribute() {
    assertThat(periodDurationConverter.convertToEntityAttribute(monthsAndDaysString)).isEqualTo(monthsAndDaysDuration);
    assertThat(periodDurationConverter.convertToEntityAttribute(threeYearsString)).isEqualTo(threeYearsDuration);
    assertThat(periodDurationConverter.convertToEntityAttribute(threeMonthsString)).isEqualTo(threeMonthsDuration);
    assertThat(periodDurationConverter.convertToEntityAttribute(threeDaysString)).isEqualTo(threeDaysPeriod);
    assertThat(periodDurationConverter.convertToEntityAttribute(threeDaysInHoursString)).isEqualTo(threeDaysDuration);
    assertThat(periodDurationConverter.convertToEntityAttribute(threeHoursString)).isEqualTo(threeHoursDuration);
    assertThat(periodDurationConverter.convertToEntityAttribute(threeMinutesString)).isEqualTo(threeMinutesDuration);
    assertThat(periodDurationConverter.convertToEntityAttribute(threeSecondsString)).isEqualTo(threeSecondsDuration);
    assertThat(periodDurationConverter.convertToEntityAttribute(null)).isNull();
  }
}
