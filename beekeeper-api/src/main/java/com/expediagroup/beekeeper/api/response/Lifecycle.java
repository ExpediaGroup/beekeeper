package com.expediagroup.beekeeper.api.response;

import java.util.Map;

import lombok.Builder;
import lombok.Value;

import com.expediagroup.beekeeper.core.model.LifecycleEventType;

@Value
@Builder
public class Lifecycle {

  LifecycleEventType lifecycleEventType;
  Map<String, String> configuration;

}
