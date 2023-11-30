/*
 *  Copyright 2021-2023 Weibo, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.weibo.rill.flow.service.component;

import com.weibo.rill.flow.service.manager.OkHttpFeaturesManager;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import org.springframework.beans.factory.FactoryBean;

import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;

import static com.weibo.rill.flow.common.model.HttpFeature.*;

public class OkHttpClientFactoryBean implements FactoryBean<OkHttpClient> {
    private long connectTimeOut;
    private long readTimeOut;
    private long writeTimeOut;
    private ConnectionPool connectionPool;

    public OkHttpClientFactoryBean() {

    }

    public OkHttpClientFactoryBean(long connectTimeOut, long readTimeOut, long writeTimeOut, ConnectionPool connectionPool) {
        this.connectTimeOut = connectTimeOut;
        this.readTimeOut = readTimeOut;
        this.writeTimeOut = writeTimeOut;
        this.connectionPool = connectionPool;
    }

    @Override
    public OkHttpClient getObject() {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();

        builder.connectTimeout(connectTimeOut, TimeUnit.MILLISECONDS)//
                .readTimeout(readTimeOut, TimeUnit.MILLISECONDS)//
                .writeTimeout(writeTimeOut, TimeUnit.MILLISECONDS);
        ServiceLoader<OkHttpFeaturesManager> okHttpFeaturesManagersLoader = ServiceLoader.load(OkHttpFeaturesManager.class);
        if (okHttpFeaturesManagersLoader.iterator().hasNext()) {
            OkHttpFeaturesManager okHttpFeaturesManager = okHttpFeaturesManagersLoader.iterator().next();
            okHttpFeaturesManager.addFeatures(builder, //
                    ACCESS_LOG, //
                    PROFILE, //
                    CONN_FAILFAST, //
                    SERVER_ERROR_FAILFAST);
        }

        if (null != connectionPool) {
            builder.connectionPool(connectionPool);
        }

        return builder.build();
    }

    @Override
    public Class<?> getObjectType() {
        return OkHttpClient.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void setConnectTimeOut(long connectTimeOut) {
        this.connectTimeOut = connectTimeOut;
    }

    public void setReadTimeOut(long readTimeOut) {
        this.readTimeOut = readTimeOut;
    }

    public void setWriteTimeOut(long writeTimeOut) {
        this.writeTimeOut = writeTimeOut;
    }

    public void setConnectionPool(ConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
    }
}
