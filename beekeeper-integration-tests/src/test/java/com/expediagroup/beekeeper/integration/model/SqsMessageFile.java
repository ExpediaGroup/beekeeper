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

import com.google.gson.*;

public abstract class SqsMessageFile {
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
}
