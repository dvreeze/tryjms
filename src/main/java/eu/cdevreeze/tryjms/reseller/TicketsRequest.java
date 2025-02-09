/*
 * Copyright 2024-2024 Chris de Vreeze
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

package eu.cdevreeze.tryjms.reseller;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import eu.cdevreeze.yaidom4j.core.NamespaceScope;
import eu.cdevreeze.yaidom4j.dom.immutabledom.Element;
import eu.cdevreeze.yaidom4j.dom.immutabledom.NodeBuilder;

/**
 * Tickets request for an event.
 *
 * @author Chris de Vreeze
 */
public record TicketsRequest(
        int eventID,
        String title,
        String date,
        String time,
        String location,
        int numberRequested
) {

    public Element toXmlElement() {
        NodeBuilder.ConciseApi api = new NodeBuilder.ConciseApi(NamespaceScope.empty());

        return api.element(
                "RequestTickets",
                ImmutableMap.of(),
                ImmutableList.of(
                        api.textElement("eventID", String.valueOf(eventID)),
                        api.textElement("title", title),
                        api.textElement("date", date),
                        api.textElement("time", time),
                        api.textElement("location", location),
                        api.textElement("numberRequested", String.valueOf(numberRequested))
                )
        );
    }
}
