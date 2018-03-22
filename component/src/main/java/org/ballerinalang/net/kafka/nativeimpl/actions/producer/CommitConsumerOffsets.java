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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.model.NativeCallableUnit;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BConnector;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BRefValueArray;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaAction;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.net.kafka.KafkaConstants;
import org.ballerinalang.net.kafka.transaction.KafkaTransactionContext;
import org.ballerinalang.util.exceptions.BallerinaException;
import org.ballerinalang.util.transactions.BallerinaTransactionContext;

/**
 * Native action ballerina.net.kafka:commitConsumerOffsets which commits the consumer fir given offsets in transaction.
 */
@BallerinaAction(packageName = "ballerina.net.kafka",
        actionName = "commitConsumerOffsets",
        connectorName = KafkaConstants.PRODUCER_CONNECTOR_NAME,
        args = {
                @Argument(name = "c",
                        type = TypeKind.CONNECTOR),
                @Argument(name = "offsets", type = TypeKind.ARRAY, elementType = TypeKind.STRUCT,
                        structType = "Offset",
                        structPackage = "ballerina.net.kafka"),
                @Argument(name = "groupID", type = TypeKind.STRING)
        },
        returnType = {@ReturnType(type = TypeKind.NONE)})
public class CommitConsumerOffsets implements NativeCallableUnit {

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {
        BConnector producerConnector = (BConnector) context.getRefArgument(0);

        BMap producerMap = (BMap) producerConnector.getRefField(2);
        BStruct producerStruct = (BStruct) producerMap.get(new BString(KafkaConstants.NATIVE_PRODUCER));

        KafkaProducer<byte[], byte[]> kafkaProducer = (KafkaProducer) producerStruct
                .getNativeData(KafkaConstants.NATIVE_PRODUCER);
        Properties producerProperties = (Properties) producerStruct
                .getNativeData(KafkaConstants.NATIVE_PRODUCER_CONFIG);

        BRefValueArray offsets = ((BRefValueArray) context.getRefArgument(1);
        String groupID = context.getStringArgument(0);
        Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap = new HashMap<>();

        for (int counter = 0; counter < offsets.size(); counter++) {
            BStruct offset = (BStruct) offsets.get(counter);
            BStruct partition = (BStruct) offset.getRefField(0);
            int offsetValue = new Long(offset.getIntField(0)).intValue();
            String topic = partition.getStringField(0);
            int partitionValue = new Long(partition.getIntField(0)).intValue();
            partitionToMetadataMap.put(new TopicPartition(topic, partitionValue), new OffsetAndMetadata(offsetValue));
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
            kafkaProducer.sendOffsetsToTransaction(partitionToMetadataMap, groupID);
        } catch (IllegalStateException | KafkaException e) {
            throw new BallerinaException("Failed to send offsets to transaction. " + e.getMessage(), e, context);
        }
        callableUnitCallback.notifySuccess();
    }

    @Override
    public boolean isBlocking() {
        return true;
    }
}
