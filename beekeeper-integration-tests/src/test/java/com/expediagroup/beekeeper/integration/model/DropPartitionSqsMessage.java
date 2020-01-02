/**
 * Copyright (C) 2019 Expedia, Inc.
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
package com.expediagroup.beekeeper.integration.model;

import java.io.IOException;
import java.net.URL;

import com.expedia.apiary.extensions.receiver.common.event.EventType;

public class DropPartitionSqsMessage extends SqsMessageFile {
    private static URL DROP_PARTITION_FILE = SqsMessageFile.class.getResource("/drop_partition.json");

    public DropPartitionSqsMessage() throws IOException {
        this.setMessageFromFile(DROP_PARTITION_FILE);
    }

    public DropPartitionSqsMessage(String partitionLocation, Boolean isUnreferenced, Boolean isExpired, Boolean isWhitelisted) throws IOException {
        this.setMessageFromFile(DROP_PARTITION_FILE);
        this.setPartitionLocation(partitionLocation);
        this.setUnreferenced(isUnreferenced);
        this.setExpired(isExpired);
        this.setWhitelisted(EventType.DROP_PARTITION, isWhitelisted);
    }

    public String getFormattedString() { return String.format(message, partitionLocation, isUnreferenced, isExpired, isWhitelisted); }

}
