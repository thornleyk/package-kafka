/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.ballerinalang.net.kafka.nativeimpl.actions.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.ballerinalang.bre.BallerinaTransactionContext;
import org.ballerinalang.bre.BallerinaTransactionManager;
import org.ballerinalang.bre.Context;
import org.ballerinalang.connector.api.AbstractNativeAction;
import org.ballerinalang.connector.api.ConnectorFuture;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BConnector;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.nativeimpl.actions.ClientConnectorFuture;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaAction;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.net.kafka.KafkaConstants;
import org.ballerinalang.net.kafka.transaction.KafkaTransactionContext;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.util.Properties;

/**
 * Native action ballerina.net.kafka:sendAdvanced send with advanced options for time stamp and key partitioning etc.
 */
@BallerinaAction(packageName = "ballerina.net.kafka",
        actionName = "sendAdvanced",
        connectorName = KafkaConstants.PRODUCER_CONNECTOR_NAME,
        args = {
                @Argument(name = "c",
                        type = TypeKind.CONNECTOR),
                @Argument(name = "record", type = TypeKind.STRUCT, structType = "ProducerRecord",
                        structPackage = "ballerina.net.kafka")
        },
        returnType = {@ReturnType(type = TypeKind.NONE)})
public class SendAdvanced extends AbstractNativeAction {

    @Override
    public ConnectorFuture execute(Context context) {
        BConnector producerConnector = (BConnector) getRefArgument(context, 0);

        BMap producerMap = (BMap) producerConnector.getRefField(2);
        BStruct producerStruct = (BStruct) producerMap.get(new BString(KafkaConstants.NATIVE_PRODUCER));

        KafkaProducer kafkaProducer = (KafkaProducer) producerStruct.getNativeData(KafkaConstants.NATIVE_PRODUCER);
        Properties producerProperties = (Properties) producerStruct
                .getNativeData(KafkaConstants.NATIVE_PRODUCER_CONFIG);

        BStruct producerRecord = ((BStruct) getRefArgument(context, 1));

        byte[] key = producerRecord.getBlobField(0);
        byte[] value = producerRecord.getBlobField(1);
        String topic = producerRecord.getStringField(0);
        Long partition = producerRecord.getIntField(0);
        Long timestamp = producerRecord.getIntField(1);

        // default value is set, so we can safely assume it s null
        if (partition == -1) {
            partition = null;
        }

        // default value is set, so we can safely assume it s null
        if (timestamp == -1) {
            timestamp = null;
        }

        ProducerRecord<byte[], byte[]> kafkaRecord;
        if (partition != null) {
            kafkaRecord =
                    new ProducerRecord(topic, new Long(partition).intValue(), timestamp, key, value);
        } else {
            kafkaRecord =
                    new ProducerRecord(topic, null, timestamp, key, value);
        }

        try {
            if (producerProperties.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG) != null
                    && context.isInTransaction()) {
                String connectorKey = producerConnector.getStringField(0);
                BallerinaTransactionManager ballerinaTxManager = context.getBallerinaTransactionManager();
                BallerinaTransactionContext regTxContext = ballerinaTxManager.getTransactionContext(connectorKey);
                if (regTxContext == null) {
                    KafkaTransactionContext txContext = new KafkaTransactionContext(kafkaProducer);
                    ballerinaTxManager.registerTransactionContext(connectorKey, txContext);
                    kafkaProducer.beginTransaction();
                }
            }
            kafkaProducer.send(kafkaRecord);
        } catch (IllegalStateException | KafkaException e) {
            throw new BallerinaException("Failed to send message. " + e.getMessage(), e, context);
        }
        ClientConnectorFuture future = new ClientConnectorFuture();
        future.notifySuccess();
        return future;
    }

}

