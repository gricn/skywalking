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

import static org.apache.skywalking.oap.server.library.util.StringUtil.isNotBlank;
import com.google.common.base.Strings;
import org.apache.skywalking.oap.server.core.analysis.Layer;
import org.apache.skywalking.oap.server.core.analysis.TimeBucket;
import org.apache.skywalking.oap.server.core.analysis.metrics.LongValueHolder;
import org.apache.skywalking.oap.server.core.analysis.metrics.Metrics;
import org.apache.skywalking.oap.server.core.analysis.metrics.WithMetadata;
import org.apache.skywalking.oap.server.core.remote.grpc.proto.RemoteData;
import org.apache.skywalking.oap.server.core.storage.annotation.Column;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode(
    callSuper = false,
    of = "uuid")
public abstract class Event extends Metrics implements ISource, WithMetadata, LongValueHolder {

    public static final String INDEX_NAME = "events";

    public static final String UUID = "uuid";

    public static final String SERVICE = "service";

    public static final String SERVICE_INSTANCE = "service_instance";

    public static final String ENDPOINT = "endpoint";

    public static final String NAME = "name";

    public static final String TYPE = "type";

    public static final String MESSAGE = "message";

    public static final String PARAMETERS = "parameters";

    public static final String START_TIME = "start_time";

    public static final String END_TIME = "end_time";

    public static final String LAYER = "layer";

    private static final int PARAMETER_MAX_LENGTH = 2000;

    @Override
    protected String id0() {
        return getUuid();
    }

    @Column(columnName = UUID)
    private String uuid;

    @Column(columnName = SERVICE)
    private String service;

    @Column(columnName = SERVICE_INSTANCE)
    private String serviceInstance;

    @Column(columnName = ENDPOINT)
    private String endpoint;

    @Column(columnName = NAME)
    private String name;

    @Column(columnName = TYPE)
    private String type;

    @Column(columnName = MESSAGE)
    private String message;

    @Column(columnName = PARAMETERS, storageOnly = true, length = PARAMETER_MAX_LENGTH)
    private String parameters;

    @Column(columnName = START_TIME)
    private long startTime;

    @Column(columnName = END_TIME)
    private long endTime;

    @Column(columnName = LAYER)
    private Layer layer;

    private transient long value = 1;

    @Override
    public boolean combine(final Metrics metrics) {
        final Event event = (Event) metrics;

        value++;

        // Set time bucket only when it's never set.
        if (getTimeBucket() <= 0) {
            if (event.getStartTime() > 0) {
                setTimeBucket(TimeBucket.getMinuteTimeBucket(event.getStartTime()));
            } else if (event.getEndTime() > 0) {
                setTimeBucket(TimeBucket.getMinuteTimeBucket(event.getEndTime()));
            }
        }

        // Set start time only when it's never set, (`start` event may come after `end` event).
        if (getStartTime() <= 0 && event.getStartTime() > 0) {
            setStartTime(event.getStartTime());
        }

        if (event.getEndTime() > 0) {
            setEndTime(event.getEndTime());
        }

        if (isNotBlank(event.getType())) {
            setType(event.getType());
        }
        if (isNotBlank(event.getMessage())) {
            setMessage(event.getMessage());
        }
        if (isNotBlank(event.getParameters())) {
            setParameters(event.getParameters());
        }
        return true;
    }

    /**
     * @since 9.0.0 Limit the length of {@link #parameters}
     */
    public void setParameters(String parameters) {
        this.parameters = parameters == null || parameters.length() <= PARAMETER_MAX_LENGTH ?
            parameters : parameters.substring(0, PARAMETER_MAX_LENGTH);
    }

    @Override
    public void calculate() {
    }

    @Override
    public Metrics toHour() {
        return null;
    }

    @Override
    public Metrics toDay() {
        return null;
    }

    @Override
    public void deserialize(final RemoteData remoteData) {
        setUuid(remoteData.getDataStrings(0));
        setService(remoteData.getDataStrings(1));
        setServiceInstance(remoteData.getDataStrings(2));
        setEndpoint(remoteData.getDataStrings(3));
        setName(remoteData.getDataStrings(4));
        setType(remoteData.getDataStrings(5));
        setMessage(remoteData.getDataStrings(6));
        setParameters(remoteData.getDataStrings(7));

        setStartTime(remoteData.getDataLongs(0));
        setEndTime(remoteData.getDataLongs(1));
        setTimeBucket(remoteData.getDataLongs(2));

        setLayer(Layer.valueOf(remoteData.getDataIntegers(0)));
    }

    @Override
    public RemoteData.Builder serialize() {
        final RemoteData.Builder builder = RemoteData.newBuilder();

        builder.addDataStrings(getUuid());
        builder.addDataStrings(getService());
        builder.addDataStrings(getServiceInstance());
        builder.addDataStrings(getEndpoint());
        builder.addDataStrings(getName());
        builder.addDataStrings(getType());
        builder.addDataStrings(getMessage());
        builder.addDataStrings(Strings.nullToEmpty(getParameters()));

        builder.addDataLongs(getStartTime());
        builder.addDataLongs(getEndTime());
        builder.addDataLongs(getTimeBucket());

        builder.addDataIntegers(getLayer().value());

        return builder;
    }

    @Override
    public int remoteHashCode() {
        return hashCode();
    }
}
