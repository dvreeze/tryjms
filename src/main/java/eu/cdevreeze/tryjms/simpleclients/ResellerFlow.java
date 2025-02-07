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

package eu.cdevreeze.tryjms.simpleclients;

import eu.cdevreeze.yaidom4j.dom.immutabledom.Element;
import eu.cdevreeze.yaidom4j.dom.immutabledom.jaxpinterop.DocumentParser;
import eu.cdevreeze.yaidom4j.dom.immutabledom.jaxpinterop.DocumentParsers;
import eu.cdevreeze.yaidom4j.dom.immutabledom.jaxpinterop.DocumentPrinter;
import eu.cdevreeze.yaidom4j.dom.immutabledom.jaxpinterop.DocumentPrinters;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSRuntimeException;
import org.xml.sax.InputSource;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * Reseller JMS flow, communicating with the event booking service.
 * <p>
 * See <a href="https://developer.ibm.com/learningpaths/ibm-mq-badge/mq-coding-challenge/">mq-coding-challenge</a>.
 *
 * @author Chris de Vreeze
 */
public class ResellerFlow {

    private static final Logger logger = LogManager.getLogManager().getLogger(Logger.GLOBAL_LOGGER_NAME);

    private static final String NEW_TICKETS_TOPIC = "newTickets";

    public static void main(String[] args) {
        ConnectionFactory cf = ConnectionFactories.newConnectionFactory();

        List<Element> eventElements = retrieveEvents(cf);

        DocumentPrinter docPrinter = DocumentPrinters.instance();

        List<String> eventsAsStrings = eventElements.stream().map(docPrinter::print).toList();

        eventsAsStrings.forEach(System.out::println);
    }

    private static List<Element> retrieveEvents(ConnectionFactory cf) {
        List<Element> results = new ArrayList<>();

        DocumentParser docParser = DocumentParsers.builder().removingInterElementWhitespace().build();

        try (JMSContext jmsContext = cf.createContext();
             JMSConsumer jmsConsumer = jmsContext.createConsumer(jmsContext.createTopic(NEW_TICKETS_TOPIC))) {

            do {
                // Polling
                logger.info("Polling");
                String msg = jmsConsumer.receiveBody(String.class, 10L * 1000);
                logger.info("Received message: " + msg);

                if (msg != null) {
                    Element xmlElem = docParser.parse(new InputSource(new StringReader(msg))).documentElement();
                    results.add(xmlElem);
                }
            } while (results.size() < 10);

            return results;
        } catch (JMSRuntimeException e) {
            logger.info("Thrown exception: " + e);
            return results;
        }
    }
}
