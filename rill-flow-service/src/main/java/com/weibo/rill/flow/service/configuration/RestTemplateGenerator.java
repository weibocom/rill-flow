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

package com.weibo.rill.flow.service.configuration;

import com.google.common.collect.Lists;
import com.weibo.rill.flow.service.component.OkHttpClientFactoryBean;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import org.springframework.http.client.OkHttp3ClientHttpRequestFactory;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.ResourceHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.http.converter.support.AllEncompassingFormHttpMessageConverter;
import org.springframework.http.converter.xml.SourceHttpMessageConverter;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;


@Component
public class RestTemplateGenerator implements BeanGenerator<RestTemplate> {

    @Override
    public boolean accept(Class<?> targetBeanType) {
        return RestTemplate.class.equals(targetBeanType);
    }

    @Override
    public RestTemplate newInstance(BeanConfig beanConfig) {
        int conTimeOutMs = Optional.ofNullable(beanConfig.getHttp()).map(BeanConfig.Http::getConTimeOutMs).orElse(500);
        int writeTimeoutMs = Optional.ofNullable(beanConfig.getHttp()).map(BeanConfig.Http::getWriteTimeoutMs).orElse(1500);
        int readTimeoutMs = Optional.ofNullable(beanConfig.getHttp()).map(BeanConfig.Http::getReadTimeoutMs).orElse(1500);
        int maxIdleConnections = Optional.ofNullable(beanConfig.getHttp()).map(BeanConfig.Http::getMaxIdleConnections).orElse(10);
        int keepAliveDurationMs = Optional.ofNullable(beanConfig.getHttp()).map(BeanConfig.Http::getKeepAliveDurationMs).orElse(10000);

        ConnectionPool connectionPool = new ConnectionPool(maxIdleConnections, keepAliveDurationMs, TimeUnit.MILLISECONDS);
        OkHttpClientFactoryBean factoryBean = new OkHttpClientFactoryBean(conTimeOutMs, readTimeoutMs, writeTimeoutMs, connectionPool);
        OkHttpClient okHttpClient = factoryBean.getObject();

        List<HttpMessageConverter<?>> messageConverters = Lists.newArrayList();
        messageConverters.add(new ByteArrayHttpMessageConverter());
        messageConverters.add(new StringHttpMessageConverter(StandardCharsets.UTF_8));
        messageConverters.add(new ResourceHttpMessageConverter());
        messageConverters.add(new SourceHttpMessageConverter<>());
        messageConverters.add(new AllEncompassingFormHttpMessageConverter());
        messageConverters.add(new MappingJackson2HttpMessageConverter());
        RestTemplate restTemplate = new RestTemplate(messageConverters);
        restTemplate.setRequestFactory(new OkHttp3ClientHttpRequestFactory(okHttpClient));
        return restTemplate;
    }
}
