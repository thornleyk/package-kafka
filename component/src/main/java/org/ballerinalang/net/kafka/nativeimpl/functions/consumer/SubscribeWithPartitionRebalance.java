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
import java.util.Collection;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.BLangVMErrors;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.bre.bvm.WorkerContext;
import org.ballerinalang.model.NativeCallableUnit;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BFunctionPointer;
import org.ballerinalang.model.values.BRefType;
import org.ballerinalang.model.values.BRefValueArray;
import org.ballerinalang.model.values.BStringArray;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.natives.AbstractNativeFunction;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.net.kafka.KafkaConstants;
import org.ballerinalang.net.kafka.KafkaUtils;
import org.ballerinalang.util.codegen.ProgramFile;
import org.ballerinalang.util.codegen.cpentries.FunctionRefCPEntry;
import org.ballerinalang.util.exceptions.BallerinaException;
import org.ballerinalang.util.program.BLangFunctions;

/**
 * Native function ballerina.net.kafka:subscribeWithPartitionRebalance subscribes to given topic array
 * with given function pointers to on revoked / on assigned events.
 */
@BallerinaFunction(packageName = "ballerina.net.kafka",
        functionName = "subscribeWithPartitionRebalance",
        receiver = @Receiver(type = TypeKind.STRUCT, structType = "Consumer",
                structPackage = "ballerina.net.kafka"),
        args = {
                @Argument(name = "c",
                        type = TypeKind.STRUCT, structType = "Consumer",
                        structPackage = "ballerina.net.kafka"),
                @Argument(name = "topics", type = TypeKind.ARRAY, elementType = TypeKind.STRING),
                @Argument(name = "onPartitionsRevoked", type = TypeKind.ANY),
                @Argument(name = "onPartitionsAssigned", type = TypeKind.ANY)
        },
        returnType = {@ReturnType(type = TypeKind.STRUCT)},
        isPublic = true)
public class SubscribeWithPartitionRebalance implements NativeCallableUnit { 

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {
        BStruct consumerStruct = (BStruct) context.getRefArgument(0);
        BStringArray topicArray = (BStringArray) getRefArgument(context, 1);
        ArrayList<String> topics = new ArrayList<String>();
        for (int counter = 0; counter < topicArray.size(); counter++) {
            topics.add(topicArray.get(counter));
        }

        FunctionRefCPEntry onPartitionsRevoked = null;
        FunctionRefCPEntry onPartitionsAssigned = null;
        if (context.getControlStackNew().getCurrentFrame().getRefLocalVars()[2] != null && context.getControlStackNew()
                .getCurrentFrame().getRefLocalVars()[2] instanceof BFunctionPointer) {
            onPartitionsRevoked = ((BFunctionPointer) getRefArgument(context, 2)).value();
        } else {
            return getBValues(BLangVMErrors.createError(context, 0,
                    "The onPartitionsRevoked function is not provided."));
        }

        if (context.getControlStackNew().getCurrentFrame().getRefLocalVars()[3] != null && context.getControlStackNew()
                .getCurrentFrame().getRefLocalVars()[3] instanceof BFunctionPointer) {
            onPartitionsAssigned = ((BFunctionPointer) getRefArgument(context, 3)).value();
        } else {
            return getBValues(BLangVMErrors.createError(context, 0,
                    "The onPartitionsAssigned function is not provided."));
        }

        ConsumerRebalanceListener listener = new KafkaRebalanceListener(context, onPartitionsRevoked,
                onPartitionsAssigned, this, consumerStruct);


        KafkaConsumer<byte[], byte[]> kafkaConsumer = (KafkaConsumer) consumerStruct
                .getNativeData(KafkaConstants.NATIVE_CONSUMER);
        if (kafkaConsumer == null) {
            throw new BallerinaException("Kafka Consumer has not been initialized properly.");
        }

        try {
            kafkaConsumer.subscribe(topics, listener);
        } catch (IllegalArgumentException |
                IllegalStateException | KafkaException e) {
                    context.setReturnValues(BLangVMErrors.createError(context, 0, e.getMessage()));
                }
        return VOID_RETURN;
    }

    /**
     * Implementation for {@link ConsumerRebalanceListener} interface from connector side.
     * We register this listener at subscription.
     *
     * {@inheritDoc}
     */
    class KafkaRebalanceListener implements ConsumerRebalanceListener {

        private Context context;
        private FunctionRefCPEntry onPartitionsRevoked;
        private FunctionRefCPEntry onPartitionsAssigned;
        private AbstractNativeFunction function;
        private BStruct consumerStruct;

        KafkaRebalanceListener(Context context,
                               FunctionRefCPEntry onPartitionsRevoked,
                               FunctionRefCPEntry onPartitionsAssigned,
                               AbstractNativeFunction function,
                               BStruct consumerStruct) {
            this.context = context;
            this.onPartitionsRevoked = onPartitionsRevoked;
            this.onPartitionsAssigned = onPartitionsAssigned;
            this.function = function;
            this.consumerStruct = consumerStruct;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            ProgramFile programFile = context.getProgramFile();
            Context childContext = new WorkerContext(programFile, context);
            BLangFunctions
                    .invokeFunction(programFile, onPartitionsRevoked.getFunctionInfo(),
                            function.getBValues(consumerStruct, getPartitionsArray(partitions)), childContext);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            ProgramFile programFile = context.getProgramFile();
            Context childContext = new WorkerContext(programFile, context);
            BLangFunctions
                    .invokeFunction(programFile, onPartitionsAssigned.getFunctionInfo(),
                            function.getBValues(consumerStruct, getPartitionsArray(partitions)), childContext);

        }

        private BRefValueArray getPartitionsArray(Collection<TopicPartition> partitions) {
            List<BStruct> assignmentList = new ArrayList<>();
            if (!partitions.isEmpty()) {
                partitions.forEach(assignment -> {
                    BStruct partitionStruct = KafkaUtils.createKafkaPackageStruct(context,
                            KafkaConstants.TOPIC_PARTITION_STRUCT_NAME);
                    partitionStruct.setStringField(0, assignment.topic());
                    partitionStruct.setIntField(0, assignment.partition());
                    assignmentList.add(partitionStruct);
                });
            }
            return new BRefValueArray(assignmentList.toArray(new BRefType[0]),
                    KafkaUtils.createKafkaPackageStruct(context,
                            KafkaConstants.TOPIC_PARTITION_STRUCT_NAME).getType());
        }

    }

    @Override
    public boolean isBlocking() {
        return true;
    }
}
