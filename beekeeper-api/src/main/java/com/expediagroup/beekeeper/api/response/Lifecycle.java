package com.expediagroup.beekeeper.api.response;

import java.util.Map;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Lifecycle {

  String lifecycleEventType;
  Map<String, String> configuration;



}
