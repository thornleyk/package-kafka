/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.ballerinalang.net.kafka.nativeimpl;

import org.ballerinalang.annotation.JavaSPIService;
import org.ballerinalang.connector.api.Annotation;
import org.ballerinalang.connector.api.BallerinaConnectorException;
import org.ballerinalang.connector.api.BallerinaServerConnector;
import org.ballerinalang.connector.api.Service;
import org.ballerinalang.net.kafka.KafkaConstants;
import org.ballerinalang.net.kafka.KafkaUtils;
import org.ballerinalang.net.kafka.api.KafkaListener;
import org.ballerinalang.net.kafka.api.KafkaServerConnector;
import org.ballerinalang.net.kafka.exception.KafkaConnectorException;
import org.ballerinalang.net.kafka.impl.KafkaListenerImpl;
import org.ballerinalang.net.kafka.impl.KafkaServerConnectorImpl;
import org.ballerinalang.util.exceptions.BallerinaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * {@code KafkaBalServerConnector} This is the Kafka implementation for the {@code BallerinaServerConnector} API.
 */
@JavaSPIService("org.ballerinalang.connector.api.BallerinaServerConnector")
public class KafkaBalServerConnector implements BallerinaServerConnector {

    private static final Logger logger = LoggerFactory.getLogger(KafkaBalServerConnector.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getProtocolPackages() {
        List<String> protocolPackages = new LinkedList<>();
        protocolPackages.add(KafkaConstants.KAFKA_NATIVE_PACKAGE);
        return protocolPackages;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void serviceRegistered(Service service) throws BallerinaConnectorException {
        List<Annotation> annotationList = service.getAnnotationList(KafkaConstants.KAFKA_NATIVE_PACKAGE,
                KafkaConstants.ANNOTATION_KAFKA_CONFIGURATION);
        String serviceName = getServiceKey(service);
        if (annotationList == null) {
            throw new BallerinaConnectorException("Unable to find the associated configuration " +
                    "annotation for given service: " + serviceName);
        }

        if (annotationList.size() > 1) {
            throw new BallerinaException(
                    "multiple service configuration annotations found in service: " + service.getName());
        }
        Annotation kafkaConfig = annotationList.get(0);

        if (kafkaConfig == null) {
            throw new BallerinaException("Error Kafka 'configuration' annotation missing in " + service.getName());
        }

        Properties configParams = KafkaUtils.processKafkaConsumerConfig(kafkaConfig);
        String serviceId = service.getName();

        try {
            KafkaListener kafkaListener = new KafkaListenerImpl(KafkaUtils.extractKafkaResource(service));
            KafkaServerConnector serverConnector = new KafkaServerConnectorImpl(serviceId,
                    configParams, kafkaListener);
            serverConnector.start();
        } catch (KafkaConnectorException e) {
            throw new BallerinaException("Error when starting to listen to the Kafka topic while "
                    + serviceId + " deployment", e);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("KafkaBalServerConnector registered successfully for service: " + serviceId);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deploymentComplete() throws BallerinaConnectorException {
        // Not required for Kafka.
        // KafkaServerConnector.start() is handled at service registration serviceRegistered(Service service)
    }

    private String getServiceKey(Service service) {
        return service.getPackage() != null ? (service.getPackage() + "_" + service.getName()) : service.getName();
    }
}
