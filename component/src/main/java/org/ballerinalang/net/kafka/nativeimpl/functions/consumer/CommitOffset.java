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

package org.ballerinalang.net.kafka.nativeimpl.functions.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.model.NativeCallableUnit;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BRefValueArray;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.net.kafka.KafkaConstants;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.util.HashMap;
import java.util.Map;

/**
 * Native function ballerina.net.kafka:commitOffset which commits given offsets of consumer to offset topic.
 */
@BallerinaFunction(packageName = "ballerina.net.kafka",
        functionName = "commitOffset",
        receiver = @Receiver(type = TypeKind.STRUCT, structType = "Consumer",
                structPackage = "ballerina.net.kafka"),
        args = {
                @Argument(name = "c",
                        type = TypeKind.STRUCT, structType = "Consumer",
                        structPackage = "ballerina.net.kafka"),
                @Argument(name = "offsets", type = TypeKind.ARRAY, elementType = TypeKind.STRUCT,
                        structType = "Offset", structPackage = "ballerina.net.kafka")
        },
        returnType = { @ReturnType(type = TypeKind.NONE)},
        isPublic = true)
public class CommitOffset implements NativeCallableUnit {

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {
        BStruct consumerStruct = (BStruct) context.getRefArgument(0);
        KafkaConsumer<byte[], byte[]> kafkaConsumer = (KafkaConsumer) consumerStruct
                .getNativeData(KafkaConstants.NATIVE_CONSUMER);
        if (kafkaConsumer == null) {
            throw new BallerinaException("Kafka Consumer has not been initialized properly.");
        }

        BRefValueArray offsets = ((BRefValueArray) context.getRefArgument(1));
        Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap = new HashMap<>();

        if (offsets != null) {
            for (int counter = 0; counter < offsets.size(); counter++) {
                BStruct offset = (BStruct) offsets.get(counter);
                BStruct partition = (BStruct) offset.getRefField(0);
                int offsetValue = new Long(offset.getIntField(0)).intValue();
                String topic = partition.getStringField(0);
                int partitionValue = new Long(partition.getIntField(0)).intValue();
                partitionToMetadataMap.put(new TopicPartition(topic, partitionValue),
                        new OffsetAndMetadata(offsetValue));
            }
        }

        try {
            kafkaConsumer.commitSync(partitionToMetadataMap);
        } catch (IllegalArgumentException | KafkaException e) {
            throw new BallerinaException("Failed to commit offsets. " + e.getMessage(), e, context);
        }
        context.setReturnValues();
    }
    
    @Override
    public boolean isBlocking() {
        return true;
    }
}
