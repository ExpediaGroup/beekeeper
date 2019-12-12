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

public class DropTableSqsMessage extends SqsMessageFile {
    private static URL DROP_TABLE_FILE = SqsMessageFile.class.getResource("/drop_table.json");

    private String message;
    private String tableLocation = "DELETEME";
    private String isUnreferenced = "false";
    private String isExpired = "false";

    public DropTableSqsMessage() throws IOException {
        message = new String(IOUtils.toByteArray(DROP_TABLE_FILE), UTF_8);
    }

    public DropTableSqsMessage(String tableLocation, Boolean isUnreferenced, Boolean isExpired) throws IOException {
        message = new String(IOUtils.toByteArray(DROP_TABLE_FILE), UTF_8);

        this.tableLocation = tableLocation;
        this.isUnreferenced = isUnreferenced.toString().toLowerCase();
        this.isExpired = isExpired.toString().toLowerCase();
    }

    public String getFormattedString() {
        return String.format(message, tableLocation, isUnreferenced, isExpired);
    }

    public void setTableLocation(String tableLocation) {
        this.tableLocation = tableLocation;
    }

    public void setUnreferenced(Boolean isUnreferenced) {
        this.isUnreferenced = isUnreferenced.toString().toLowerCase();
    }

    public void setExpired(Boolean isExpired) {
        this.isExpired = isExpired.toString().toLowerCase();
    }
}
