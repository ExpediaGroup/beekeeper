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

import java.time.LocalDateTime;

public class PartitionInfo {
  private final String path;
  private final LocalDateTime createTime;

  public PartitionInfo(String path, LocalDateTime createTime) {
    if (path == null) {
        throw new IllegalArgumentException("Path cannot be null");
    }
    if (createTime == null) {
        throw new IllegalArgumentException("CreateTime cannot be null");
    }
    this.path = path;
    this.createTime = createTime;
  }

  public String getPath() {
    return path;
  }

  public LocalDateTime getCreateTime() {
    return createTime;
  }
}
