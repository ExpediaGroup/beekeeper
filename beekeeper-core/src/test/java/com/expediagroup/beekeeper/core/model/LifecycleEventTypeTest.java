package com.expediagroup.beekeeper.core.model;

import static org.assertj.core.api.Assertions.assertThat;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class LifecycleEventTypeTest {

  private static final String TRUE_STR = "true";
  private static final String FALSE_STR = "false";

  @Test
  public void shouldValidateUnreferencedParameterTrue() {
    assertThat(UNREFERENCED.getBoolean(Map.of(UNREFERENCED.getTableParameterName(), TRUE_STR))).isTrue();
  }

  @Test
  public void shouldValidateUnreferencedParameterFalse() {
    assertThat(UNREFERENCED.getBoolean(Map.of(UNREFERENCED.getTableParameterName(), FALSE_STR))).isFalse();
  }

  @ParameterizedTest
  @ValueSource(strings = { "", " ", "beekeeper.remove.unref.data", "some.foo" })
  public void shouldIgnoreInvalidUnreferencedParameters(String param) {
    assertThat(UNREFERENCED.getBoolean(Map.of(param, TRUE_STR))).isFalse();
  }
}