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

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.BLangVMErrors;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.model.NativeCallableUnit;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BRefType;
import org.ballerinalang.model.values.BRefValueArray;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.net.kafka.KafkaConstants;
import org.ballerinalang.net.kafka.KafkaUtils;
import org.ballerinalang.util.exceptions.BallerinaException;

/**
 * Native function ballerina.net.kafka:getTopicPartitions returns partition array for given topic.
 */
@BallerinaFunction(packageName = "ballerina.net.kafka",
        functionName = "getTopicPartitions",
        receiver = @Receiver(type = TypeKind.STRUCT, structType = "Consumer",
                structPackage = "ballerina.net.kafka"),
        args = {
                @Argument(name = "c",
                        type = TypeKind.STRUCT, structType = "Consumer",
                        structPackage = "ballerina.net.kafka"),
                @Argument(name = "topic", type = TypeKind.STRING)
        },
        returnType = {@ReturnType(type = TypeKind.ARRAY, elementType = TypeKind.STRUCT, structType = "TopicPartition",
                structPackage = "ballerina.net.kafka"),
                @ReturnType(type = TypeKind.STRUCT)},
        isPublic = true)
public class GetTopicPartitions implements NativeCallableUnit {

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {
        BStruct consumerStruct = (BStruct) context.getRefArgument(0);
        String topic = context.getStringArgument(0);

        KafkaConsumer<byte[], byte[]> kafkaConsumer = (KafkaConsumer) consumerStruct
                .getNativeData(KafkaConstants.NATIVE_CONSUMER);
        if (kafkaConsumer == null) {
            throw new BallerinaException("Kafka Consumer has not been initialized properly.");
        }

        try {
            List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);
            List<BStruct> infoList = new ArrayList<>();
            if (!partitionInfos.isEmpty()) {
                partitionInfos.forEach(partitionInfo -> {
                    BStruct partitionStruct = KafkaUtils.createKafkaPackageStruct(context,
                            KafkaConstants.TOPIC_PARTITION_STRUCT_NAME);
                    partitionStruct.setStringField(0, partitionInfo.topic());
                    partitionStruct.setIntField(0, partitionInfo.partition());
                    infoList.add(partitionStruct);
                });
            }
            context.setReturnValues(new BRefValueArray(infoList.toArray(new BRefType[0]),
                    KafkaUtils.createKafkaPackageStruct(context,
                            KafkaConstants.TOPIC_PARTITION_STRUCT_NAME).getType()));
        } catch (KafkaException e) {
                context.setReturnValues(BLangVMErrors.createError(context, 0, e.getMessage()));
        }
    }

    @Override
    public boolean isBlocking() {
        return true;
    }
}
