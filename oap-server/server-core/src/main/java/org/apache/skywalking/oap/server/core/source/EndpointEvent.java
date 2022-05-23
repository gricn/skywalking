/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.oap.server.core.source;

import static org.apache.skywalking.oap.server.core.source.DefaultScopeDefine.ENDPOINT_CATALOG_NAME;
import static org.apache.skywalking.oap.server.core.source.DefaultScopeDefine.ENDPOINT_EVENT;
import org.apache.skywalking.oap.server.core.analysis.IDManager;
import org.apache.skywalking.oap.server.core.analysis.Layer;
import org.apache.skywalking.oap.server.core.analysis.MetricsExtension;
import org.apache.skywalking.oap.server.core.analysis.Stream;
import org.apache.skywalking.oap.server.core.analysis.metrics.MetricsMetaInfo;
import org.apache.skywalking.oap.server.core.analysis.worker.MetricsStreamProcessor;
import org.apache.skywalking.oap.server.core.storage.type.Convert2Entity;
import org.apache.skywalking.oap.server.core.storage.type.Convert2Storage;
import org.apache.skywalking.oap.server.core.storage.type.StorageBuilder;

@ScopeDeclaration(id = ENDPOINT_EVENT, name = "EndpointEvent", catalog = ENDPOINT_CATALOG_NAME)
@ScopeDefaultColumn.VirtualColumnDefinition(fieldName = "entityId", columnName = "entity_id", isID = true, type = String.class)
@Stream(name = Event.INDEX_NAME, scopeId = ENDPOINT_EVENT, builder = EndpointEvent.Builder.class, processor = MetricsStreamProcessor.class)
@MetricsExtension(supportDownSampling = false, supportUpdate = true)
public class EndpointEvent extends Event {

    @Override
    public MetricsMetaInfo getMeta() {
        int scope = DefaultScopeDefine.ENDPOINT;
        String id = getEntityId();
        return new MetricsMetaInfo(getName(), scope, id);
    }

    @Override
    public String getEntityId() {
        final String serviceId = IDManager.ServiceID.buildId(getService(), true);
        String id = IDManager.EndpointID.buildId(serviceId, getEndpoint());
        return id;
    }

    public static class Builder implements StorageBuilder<EndpointEvent> {
        @Override
        public EndpointEvent storage2Entity(final Convert2Entity converter) {
            EndpointEvent record = new EndpointEvent();
            record.setUuid((String) converter.get(UUID));
            record.setService((String) converter.get(SERVICE));
            record.setServiceInstance((String) converter.get(SERVICE_INSTANCE));
            record.setEndpoint((String) converter.get(ENDPOINT));
            record.setName((String) converter.get(NAME));
            record.setType((String) converter.get(TYPE));
            record.setMessage((String) converter.get(MESSAGE));
            record.setParameters((String) converter.get(PARAMETERS));
            record.setStartTime(((Number) converter.get(START_TIME)).longValue());
            record.setEndTime(((Number) converter.get(END_TIME)).longValue());
            record.setTimeBucket(((Number) converter.get(TIME_BUCKET)).longValue());
            if (converter.get(LAYER) != null) {
                record.setLayer(Layer.valueOf(((Number) converter.get(LAYER)).intValue()));
            }
            return record;
        }

        @Override
        public void entity2Storage(final EndpointEvent storageData, final Convert2Storage converter) {
            converter.accept(UUID, storageData.getUuid());
            converter.accept(SERVICE, storageData.getService());
            converter.accept(SERVICE_INSTANCE, storageData.getServiceInstance());
            converter.accept(ENDPOINT, storageData.getEndpoint());
            converter.accept(NAME, storageData.getName());
            converter.accept(TYPE, storageData.getType());
            converter.accept(MESSAGE, storageData.getMessage());
            converter.accept(PARAMETERS, storageData.getParameters());
            converter.accept(START_TIME, storageData.getStartTime());
            converter.accept(END_TIME, storageData.getEndTime());
            converter.accept(TIME_BUCKET, storageData.getTimeBucket());
            Layer layer = storageData.getLayer();
            converter.accept(LAYER, layer != null ? layer.value() : Layer.UNDEFINED.value());
        }
    }

    @Override
    public int scope() {
        return ENDPOINT_EVENT;
    }
}
