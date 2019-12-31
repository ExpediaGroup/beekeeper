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
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.URL;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CreateTableSqsMessage extends SqsMessageFile {
    private static URL CREATE_TABLE_FILE = SqsMessageFile.class.getResource("/create_table.json");

    public CreateTableSqsMessage() throws IOException {
        this.setMessageFromFile(CREATE_TABLE_FILE);
    }

    public CreateTableSqsMessage(String tableLocation, Boolean isUnreferenced, Boolean isExpired, Boolean isWhitelisted) throws IOException {
        this.setMessageFromFile(CREATE_TABLE_FILE);
        this.setTableLocation(tableLocation);
        this.setUnreferenced(isUnreferenced);
        this.setExpired(isExpired);
        this.setWhitelisted(EventType.CREATE_TABLE, isWhitelisted);
    }

    public String getFormattedString() {
        return String.format(message, tableLocation, isUnreferenced, isExpired, isWhitelisted);
    }
}
