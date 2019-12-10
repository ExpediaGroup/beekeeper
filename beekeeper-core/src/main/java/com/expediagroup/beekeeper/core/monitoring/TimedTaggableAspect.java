/**
 * This class is loosely based on {@link io.micrometer.core.aop.TimedAspect}. It has been adapted in order to create
 * a custom tag for the timer metric that is registered each time it is called.
 */
package com.expediagroup.beekeeper.core.monitoring;

import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

/**
 * Aspect for intercepting methods annotated with {@link TimedTaggable}.
 *
 * The method can have any number of arguments but a {@link Taggable} must be the first. Will add the custom tag to the
 * timer metric.
 */
@Aspect
@Component
public class TimedTaggableAspect {

  public static final String EXCEPTION_TAG = "exception";

  private MeterRegistry meterRegistry;

  @Autowired
  public TimedTaggableAspect(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
  }

  @Around("@annotation(timedTaggable) && args(taggable,..)")
  public Object time(
      ProceedingJoinPoint pjp,
      Taggable taggable,
      TimedTaggable timedTaggable)
    throws Throwable {
    String metricName = timedTaggable.value();
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
          .tags(metricTags(pjp, taggable.getMetricTag()))
          .register(meterRegistry));
      } catch (Exception e) {
        // ignoring on purpose
      }
    }
  }

  private Tags metricTags(ProceedingJoinPoint pjp, MetricTag metricTag) {
    Tags tags = Tags.of("class", pjp.getStaticPart().getSignature().getDeclaringTypeName(),
      "method", pjp.getStaticPart().getSignature().getName());
    if (!StringUtils.isBlank(metricTag.getTag()) && !StringUtils.isBlank(metricTag.getKey())) {
      tags = tags.and(metricTag.getKey(), metricTag.getTag());
    }
    return tags;
  }

}
