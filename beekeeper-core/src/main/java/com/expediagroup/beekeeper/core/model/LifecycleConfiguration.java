package com.expediagroup.beekeeper.core.model;

import java.util.List;

public class LifecycleConfiguration {

  boolean removeUnreferencedData;
  String UnreferencedDataRetentionPeriod;
  List<String> hiveEventWhitelist;

}
