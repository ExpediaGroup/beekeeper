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

import com.expedia.apiary.extensions.receiver.common.event.EventType;

import java.io.IOException;
import java.net.URL;

public class AddPartitionSqsMessage extends SqsMessageFile {
    private static URL ADD_PARTITION_FILE = SqsMessageFile.class.getResource("/add_partition.json");

    public AddPartitionSqsMessage() throws IOException {
        this.setMessageFromFile(ADD_PARTITION_FILE);
    }

    public AddPartitionSqsMessage(String partitionLocation, Boolean isUnreferenced, Boolean isExpired, Boolean isWhitelisted) throws IOException {
        this.setMessageFromFile(ADD_PARTITION_FILE);
        this.setPartitionLocation(partitionLocation);
        this.setUnreferenced(isUnreferenced);
        this.setExpired(isExpired);
        this.setWhitelisted(EventType.ADD_PARTITION, isWhitelisted);
    }

    public String getFormattedString() { return String.format(message, partitionLocation, isUnreferenced, isExpired, isWhitelisted); }


}
