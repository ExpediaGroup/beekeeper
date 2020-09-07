/**
 * Copyright (C) 2019-2020 Expedia, Inc.
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
package com.expediagroup.beekeeper.cleanup.hive;

import java.util.function.Supplier;

import com.expediagroup.beekeeper.cleanup.metadata.CleanerClientFactory;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

public class HiveClientFactory implements CleanerClientFactory {

  private Supplier<CloseableMetaStoreClient> metaStoreClientSupplier;
  private boolean dryRunEnabled;

  public HiveClientFactory(Supplier<CloseableMetaStoreClient> metaStoreClientSupplier, boolean dryRunEnabled) {
    this.metaStoreClientSupplier = metaStoreClientSupplier;
    this.dryRunEnabled = dryRunEnabled;
  }

  @Override
  public HiveClient newInstance(){
    return new HiveClient(metaStoreClientSupplier.get(), dryRunEnabled);
  }

}
