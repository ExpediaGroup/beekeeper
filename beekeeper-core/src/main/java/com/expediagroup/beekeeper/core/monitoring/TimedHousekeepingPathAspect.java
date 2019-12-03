/**
 * This class is loosely based on {@link io.micrometer.core.aop.TimedAspect}. It has been adapted in order to create
 * a custom tag for the timer metric that is registered each time it is called.
 */
package com.expediagroup.beekeeper.core.monitoring;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;
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
  public Object time(
      ProceedingJoinPoint pjp,
      HousekeepingPath path,
      TimedHousekeepingPath timedHousekeepingPath)
    throws Throwable {
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
          .tags(metricTags(pjp, path))
          .register(meterRegistry));
      } catch (Exception e) {
        // ignoring on purpose
      }
    }
  }

  private Tags metricTags(ProceedingJoinPoint pjp, HousekeepingPath path) {
    return Tags.of(
      "class", pjp.getStaticPart().getSignature().getDeclaringTypeName(),
      "method", pjp.getStaticPart().getSignature().getName(),
      "table", String.join(".", path.getDatabaseName(), path.getTableName()));
  }

}
