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

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
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
 * Native function ballerina.net.kafka:poll poll the broker to retrieve messages within given timeout.
 */
@BallerinaFunction(packageName = "ballerina.net.kafka",
        functionName = "poll",
        receiver = @Receiver(type = TypeKind.STRUCT, structType = "Consumer",
                structPackage = "ballerina.net.kafka"),
        args = {
                @Argument(name = "c",
                        type = TypeKind.STRUCT, structType = "Consumer",
                        structPackage = "ballerina.net.kafka"),
                @Argument(name = "timeout", type = TypeKind.INT)
        },
        returnType = {@ReturnType(type = TypeKind.ARRAY, elementType = TypeKind.STRUCT,
                structPackage = "ballerina.net.kafka", structType = "ConsumerRecord"),
                @ReturnType(type = TypeKind.STRUCT)},
        isPublic = true)
public class Poll implements NativeCallableUnit {

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {
        BStruct consumerStruct = (BStruct) context.getRefArgument(0);

        KafkaConsumer<byte[], byte[]> kafkaConsumer = (KafkaConsumer) consumerStruct
                .getNativeData(KafkaConstants.NATIVE_CONSUMER);
        if (kafkaConsumer == null) {
            throw new BallerinaException("Kafka Consumer has not been initialized properly.");
        }

        long timeout = context.getIntArgument(0);
        List<BStruct> recordsList = new ArrayList<>();

        try {
            ConsumerRecords<byte[], byte[]> recordsRetrieved = kafkaConsumer.poll(timeout);
            if (!recordsRetrieved.isEmpty()) {
                recordsRetrieved.forEach(record -> {
                    BStruct recordStruct = KafkaUtils.createKafkaPackageStruct(context,
                            KafkaConstants.CONSUMER_RECORD_STRUCT_NAME);
                    recordStruct.setBlobField(0, record.key());
                    recordStruct.setBlobField(1, record.value());
                    recordStruct.setIntField(0, record.offset());
                    recordStruct.setIntField(1, record.partition());
                    recordStruct.setIntField(2, record.timestamp());
                    recordStruct.setStringField(0, record.topic());
                    recordsList.add(recordStruct);
                });
            }
            context.setReturnValues(new BRefValueArray(recordsList.toArray(new BRefType[0]),
                    KafkaUtils.createKafkaPackageStruct(context,
                            KafkaConstants.CONSUMER_RECORD_STRUCT_NAME).getType()));
        } catch (IllegalStateException |
                IllegalArgumentException | KafkaException e) {
                        context.setReturnValues(BLangVMErrors.createError(context, 0, e.getMessage()));
                }
    }

    @Override
    public boolean isBlocking() {
        return true;
    }
}
