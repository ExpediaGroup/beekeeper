/**
 * Copyright (C) 2019-2025 Expedia, Inc.
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
package com.expediagroup.beekeeper.scheduler.hive;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.iterator.PartitionIterator;

public class PartitionIteratorFactory {

  private static final short MAX_PARTITIONS = (short) 1000;

  public PartitionIterator newInstance(CloseableMetaStoreClient client, Table table) throws TException {
    return new PartitionIterator(client, table, MAX_PARTITIONS, PartitionIterator.Ordering.NATURAL);
  }
}
