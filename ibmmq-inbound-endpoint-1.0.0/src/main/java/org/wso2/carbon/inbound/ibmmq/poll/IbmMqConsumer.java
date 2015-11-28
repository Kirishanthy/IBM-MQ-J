/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.wso2.carbon.inbound.ibmmq.poll;

import com.ibm.jms.JMSTextMessage;
import com.ibm.mq.jms.MQQueue;
import com.ibm.mq.jms.MQQueueConnection;
import com.ibm.mq.jms.MQQueueConnectionFactory;
import com.ibm.mq.jms.MQQueueReceiver;
import com.ibm.mq.jms.MQQueueSession;
import com.ibm.msg.client.wmq.WMQConstants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.SynapseException;
import org.apache.synapse.core.SynapseEnvironment;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericPollingConsumer;

import javax.jms.JMSException;
import javax.jms.Session;
import java.util.Properties;

public class IbmMqConsumer extends GenericPollingConsumer {
    private static final Log log = LogFactory.getLog(IbmMqConsumer.class);
    private boolean isConnected=false;
    MQQueueReceiver receiver;

    public IbmMqConsumer(Properties ibmMqProperties, String name, SynapseEnvironment synapseEnvironment,
                         long scanInterval, String injectingSeq, String onErrorSeq, boolean coordination,
                         boolean sequential) {
        super(ibmMqProperties, name, synapseEnvironment, scanInterval, injectingSeq, onErrorSeq, coordination,
                sequential);
        log.info("Initialized the IBM MQ consumer");
    }

    public Object poll() {
        if (!isConnected) {
            setupConnection();
        } if (isConnected) {
            messageFromQueue();
        } else{
            log.info("poll");
        }
        return null;
    }

    /**
     * Injecting the IBM MQ to the sequence
     *
     * @param message the IBM MQ response message
     */
    public void injectIbmMqMessage(String message) {
        if (injectingSeq != null) {
            String contentType = properties.getProperty(ibmMqConstant.CONTENT_TYPE);
            injectMessage(message, contentType);
            if (log.isDebugEnabled()) {
                log.debug("Injecting IBM MQ  message to the sequence : " + injectingSeq);
            }
        } else {
            handleException("The Sequence is not found");
        }
    }

    /**
     * Setting up the connection
     */
    private void setupConnection() {
        if (log.isDebugEnabled()) {
            log.debug("Starting to setup the connection with the IBM MQ");
        }
        String hostname = properties.getProperty(ibmMqConstant.HOST_NAME);
        String channel = properties.getProperty(ibmMqConstant.CHANNEL);
        String qName = properties.getProperty(ibmMqConstant.MQ_QUEUE);
        String qManager = properties.getProperty(ibmMqConstant.MQ_QMGR);
        String port = properties.getProperty(ibmMqConstant.PORT);
        String userName = properties.getProperty(ibmMqConstant.USER_ID);
        String password = properties.getProperty(ibmMqConstant.PASSWORD);
        try {
            MQQueueConnectionFactory connectionFactory = new MQQueueConnectionFactory();
            connectionFactory.setHostName(hostname);
            connectionFactory.setPort(Integer.parseInt(port));
            connectionFactory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
            connectionFactory.setQueueManager(qManager);
            connectionFactory.setChannel(channel);
            MQQueueConnection connection = (MQQueueConnection) connectionFactory.createQueueConnection(userName,password);
            MQQueueSession session = (MQQueueSession) connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            MQQueue queue = (MQQueue) session.createQueue("queue:///"+qName);
            receiver = (MQQueueReceiver) session.createReceiver(queue);
            connection.start();
            log.info("IBM MQ "+qManager+" queue manager successfully connected");
            isConnected = true;
        } catch (JMSException e) {
            handleException(e.getMessage());
            isConnected = false;
        }
    }

    /**
     * Receiving message from queue
     */
    private void messageFromQueue() {
        try {
            JMSTextMessage receivedMessage = (JMSTextMessage) receiver.receive();
            injectIbmMqMessage(receivedMessage.getText());
        } catch (JMSException e) {
            handleException(e.getMessage());
        }
    }

    private void handleException(String msg) {
        log.error(msg);
        throw new SynapseException(msg);
    }
}