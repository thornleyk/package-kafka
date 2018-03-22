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
import org.apache.kafka.common.KafkaException;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.model.NativeCallableUnit;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BConnector;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaAction;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.net.kafka.KafkaConstants;
import org.ballerinalang.util.exceptions.BallerinaException;

/**
 * Native action ballerina.net.kafka:close closes producer instance.
 */
@BallerinaAction(packageName = "ballerina.net.kafka",
        actionName = "close",
        connectorName = KafkaConstants.PRODUCER_CONNECTOR_NAME,
        args = {
                @Argument(name = "c",
                        type = TypeKind.CONNECTOR)
        },
        returnType = {@ReturnType(type = TypeKind.NONE)})
public class Close implements NativeCallableUnit {

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {
        BConnector producerConnector = (BConnector) context.getRefArgument(0);

        BMap producerMap = (BMap) producerConnector.getRefField(2);
        BStruct producerStruct = (BStruct) producerMap.get(new BString(KafkaConstants.NATIVE_PRODUCER));

        KafkaProducer<byte[], byte[]> kafkaProducer =
                (KafkaProducer) producerStruct.getNativeData(KafkaConstants.NATIVE_PRODUCER);
        try {
            kafkaProducer.close();
        } catch (KafkaException e) {
            throw new BallerinaException("Failed to close the producer " + e.getMessage(), e, context);
        }
        callableUnitCallback.notifySuccess();
    }

    @Override
    public boolean isBlocking() {
        return true;
    }
}
