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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;

public class PartitionInfoTest {

  @Test
  public void validConstructorParams() {
    String path = "test/path";
    LocalDateTime createTime = LocalDateTime.now();
    
    PartitionInfo partitionInfo = new PartitionInfo(path, createTime);
    
    assertThat(partitionInfo.getPath()).isEqualTo(path);
    assertThat(partitionInfo.getCreateTime()).isEqualTo(createTime);
  }
  
  @Test
  public void nullPathThrowsException() {
    LocalDateTime createTime = LocalDateTime.now();
    
    assertThatThrownBy(() -> new PartitionInfo(null, createTime))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Path cannot be null");
  }
  
  @Test
  public void nullCreateTimeThrowsException() {
    String path = "test/path";
    
    assertThatThrownBy(() -> new PartitionInfo(path, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("CreateTime cannot be null");
  }
}
