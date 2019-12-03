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
package com.expediagroup.beekeeper.core.monitoring;

import java.util.function.Function;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import com.expediagroup.beekeeper.core.model.HousekeepingPath;

/**
 * Aspect for intercepting methods annotated with {@link TimedHousekeepingPath}.
 *
 * The method can have any number of arguments but a {@link HousekeepingPath} must be the first. Will add the fully
 * qualified table name to the timer metric as a tag.
 */
@Aspect
@Component
public class TimedHousekeepingPathAspect {

  public static final String EXCEPTION_TAG = "exception";

  private MeterRegistry meterRegistry;

  @Autowired
  public TimedHousekeepingPathAspect(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
  }

  @Around("@annotation(timedHousekeepingPath) && args(path,..)")
  public Object time(ProceedingJoinPoint pjp, HousekeepingPath path, TimedHousekeepingPath timedHousekeepingPath) throws Throwable {
    String metricName = timedHousekeepingPath.value();
    Timer.Sample sample = Timer.start(meterRegistry);
    String exceptionClass = "none";
    try {
      return pjp.proceed();
    } catch (Exception ex) {
      exceptionClass = ex.getClass().getSimpleName();
      throw ex;
    } finally {
      try {
        sample.stop(Timer.builder(metricName)
          .tags(EXCEPTION_TAG, exceptionClass)
          .tags(metricTags(path).apply(pjp))
          .register(meterRegistry));
      } catch (Exception e) {
        // ignoring on purpose
      }
    }
  }

  private Function<ProceedingJoinPoint, Iterable<Tag>> metricTags(HousekeepingPath path) {
    return (pjp -> Tags.of(
      "class", pjp.getStaticPart().getSignature().getDeclaringTypeName(),
      "method", pjp.getStaticPart().getSignature().getName(),
      "table", String.join(".", path.getDatabaseName(), path.getTableName())));
  }

}
