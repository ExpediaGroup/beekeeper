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
import com.google.gson.*;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.URL;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class SqsMessageFile {
    protected String message;
    protected String partitionLocation = "DELETEME";
    protected String oldPartitionLocation = "DELETEME";
    protected String tableLocation = "DELETEME";
    protected String oldTableLocation = "DELETEME";
    protected String isUnreferenced = "false";
    protected String isExpired = "false";
    protected String isWhitelisted = "";


    public abstract String getFormattedString();

    public JsonObject getNestedJsonObject()  {
        JsonParser parser = new JsonParser();
        JsonObject json = parser.parse(getFormattedString()).getAsJsonObject();

        String message = json.get("Message").getAsString();
        JsonObject nestedMessageJsonObject = parser.parse(message).getAsJsonObject();
        return nestedMessageJsonObject;
    }

    public String prettyPrintedMessageContents() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String prettyJson = gson.toJson(getNestedJsonObject());
        return prettyJson;
    }

    public void setMessageFromFile(URL filePath) throws IOException {
        this.message = new String(IOUtils.toByteArray(filePath), UTF_8);
    }

    public void setWhitelisted(EventType eventType, Boolean isWhitelisted) {
        this.isWhitelisted = isWhitelisted ? eventType.toString() : "";
    }

    public void setExpired(Boolean isExpired) {
        this.isExpired = isExpired.toString().toLowerCase();
    }

    public void setUnreferenced(Boolean isUnreferenced) {
        this.isUnreferenced = isUnreferenced.toString().toLowerCase();
    }

    public void setPartitionLocation(String partitionLocation) {
        this.partitionLocation = partitionLocation;
    }

    public void setOldPartitionLocation(String oldPartitionLocation) {
        this.oldPartitionLocation = oldPartitionLocation;
    }

    public void setTableLocation(String tableLocation) {
        this.tableLocation = tableLocation;
    }

    public void setOldTableLocation(String oldTableLocation) {
        this.oldTableLocation = oldTableLocation;
    }

    public String getPartitionLocation() {
        return partitionLocation;
    }

    public String getOldPartitionLocation() {
        return oldPartitionLocation;
    }

    public String getTableLocation() {
        return tableLocation;
    }

    public String getOldTableLocation() {
        return oldTableLocation;
    }

    public String getIsUnreferenced() {
        return isUnreferenced;
    }

    public String getIsExpired() {
        return isExpired;
    }

    public String getIsWhitelisted() {
        return isWhitelisted;
    }

}
