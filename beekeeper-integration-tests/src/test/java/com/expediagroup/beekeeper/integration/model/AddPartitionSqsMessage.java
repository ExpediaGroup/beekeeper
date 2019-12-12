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

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.URL;

import static java.nio.charset.StandardCharsets.UTF_8;

public class AddPartitionSqsMessage extends SqsMessageFile {
    private static URL ADD_PARTITION_FILE = SqsMessageFile.class.getResource("/add_partition.json");

    private String contents;
    private String partitionLocation = "DELETEME";
    private String isUnreferenced = "false";
    private String isExpired = "false";

    public AddPartitionSqsMessage() throws IOException {
        contents = new String(IOUtils.toByteArray(ADD_PARTITION_FILE), UTF_8);
    }

    public AddPartitionSqsMessage(String partitionLocation, Boolean isUnreferenced, Boolean isExpired) throws IOException {
        contents = new String(IOUtils.toByteArray(ADD_PARTITION_FILE), UTF_8);

        this.partitionLocation = partitionLocation;
        this.isUnreferenced = isUnreferenced.toString().toLowerCase();
        this.isExpired = isExpired.toString().toLowerCase();
    }

    public String getFormattedString() {
        return String.format(contents, partitionLocation, isUnreferenced, isExpired);
    }

    public void setPartitionLocation(String partitionLocation) {
        this.partitionLocation = partitionLocation;
    }

    public void setUnreferenced(Boolean isUnreferenced) {
        this.isUnreferenced = isUnreferenced.toString().toLowerCase();
    }

    public void setExpired(Boolean isExpired) {
        this.isExpired = isExpired.toString().toLowerCase();
    }
}
