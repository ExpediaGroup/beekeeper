/**
 * Copyright (C) 2019-2020 Expedia, Inc.
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
package com.expediagroup.beekeeper.scheduler.apiary.filter;

import org.springframework.stereotype.Component;

import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;

import com.expediagroup.beekeeper.core.model.LifecycleEventType;

@Component
public class EventTypeListenerEventFilter implements ListenerEventFilter {

  @Override
  public boolean filter(ListenerEvent listenerEvent, LifecycleEventType lifecycleEventType) {
    Class<? extends ListenerEvent> eventClass = listenerEvent.getEventType().eventClass();
    return !(AlterPartitionEvent.class.equals(eventClass) ||
        AlterTableEvent.class.equals(eventClass) ||
        DropPartitionEvent.class.equals(eventClass) ||
        DropTableEvent.class.equals(eventClass));
  }
}
