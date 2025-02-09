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

import static eu.cdevreeze.yaidom4j.dom.immutabledom.ElementPredicates.hasName;

/**
 * Event for which tickets can be bought.
 *
 * @author Chris de Vreeze
 */
public record Event(
        int eventID,
        String title,
        String date,
        String time,
        String location,
        int capacity
) {

    public Element toXmlElement() {
        NodeBuilder.ConciseApi api = new NodeBuilder.ConciseApi(NamespaceScope.empty());

        return api.element(
                "Event",
                ImmutableMap.of(),
                ImmutableList.of(
                        api.textElement("eventID", String.valueOf(eventID)),
                        api.textElement("title", title),
                        api.textElement("date", date),
                        api.textElement("time", time),
                        api.textElement("location", location),
                        api.textElement("capacity", String.valueOf(capacity))
                )
        );
    }

    public static Event fromXmlElement(Element element) {
        // Maybe make these similar queries easier in yaidom4j?
        return new Event(
                element.childElementStream(hasName("eventID"))
                        .map(Element::text)
                        .mapToInt(Integer::parseInt)
                        .findFirst()
                        .orElseThrow(),
                element.childElementStream(hasName("title"))
                        .map(Element::text)
                        .findFirst()
                        .orElseThrow(),
                element.childElementStream(hasName("date"))
                        .map(Element::text)
                        .findFirst()
                        .orElseThrow(),
                element.childElementStream(hasName("time"))
                        .map(Element::text)
                        .findFirst()
                        .orElseThrow(),
                element.childElementStream(hasName("location"))
                        .map(Element::text)
                        .findFirst()
                        .orElseThrow(),
                element.childElementStream(hasName("capacity"))
                        .map(Element::text)
                        .mapToInt(Integer::parseInt)
                        .findFirst()
                        .orElseThrow()
        );
    }
}
