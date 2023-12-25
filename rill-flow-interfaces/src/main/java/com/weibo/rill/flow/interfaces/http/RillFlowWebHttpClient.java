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

package com.weibo.rill.flow.interfaces.http;

import com.weibo.rill.flow.interfaces.utils.WebHttpClientUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class RillFlowWebHttpClient implements FlowHttpClient {

    private static final CloseableHttpClient DEFAULT_HTTP_CLIENT = HttpClients.createDefault();

    private CloseableHttpClient httpClient;

    public RillFlowWebHttpClient() {
        this.httpClient = DEFAULT_HTTP_CLIENT;
    }

    public RillFlowWebHttpClient(CloseableHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public String get(String url, Map<String, String> header, Map<String, Object> param, Long uid) {
        try {
            return executeGetRequest(url, param, header);
        } catch (Exception e) {
            log.error("http get error. url:{}, header:{}, param:{}, uid:{}", url, header, param, uid, e);
        }
        return null;
    }

    @Override
    public String postWithBody(String url, Map<String, String> header, Map<String, Object> param, String body, Long uid) {
        try {
            HttpPost httpPost = WebHttpClientUtil.httpPost(url, header, param, body);
            CloseableHttpResponse response = httpClient.execute(httpPost);

            HttpEntity entity = response.getEntity();
            return httpEntityToString(entity);
        } catch (Exception e) {
            log.error("http post with body error. url:{}, header:{}, param:{}, body:{}, uid:{}", url, header, param, body, uid, e);
        }
        return null;
    }

    @Override
    public String post(String url, Map<String, String> header, Map<String, Object> param, Long uid) {
        return post(url, header, param);
    }

    @Override
    public String post(String url, Map<String, String> header, Map<String, Object> param) {
        try {
            return executePostRequest(url, param, header);
        } catch (Exception e) {
            log.error("http post error. url:{}, header:{}, param:{}", url, header, param, e);
        }
        return null;
    }

    private String executeGetRequest(String url, Map<String, Object> param, Map<String, String> header) throws IOException {
        HttpGet httpGet = WebHttpClientUtil.httpGet(url, param, header);
        CloseableHttpResponse response = httpClient.execute(httpGet);

        HttpEntity entity = response.getEntity();
        return httpEntityToString(entity);
    }

    private String executePostRequest(String url, Map<String, Object> param, Map<String, String> header) throws IOException {
        HttpPost httpPost = WebHttpClientUtil.httpPost(url, header, param, null);
        CloseableHttpResponse response = httpClient.execute(httpPost);

        HttpEntity entity = response.getEntity();
        return httpEntityToString(entity);
    }

    private static String httpEntityToString(HttpEntity entity) throws IOException {
        if (entity != null) {
            return EntityUtils.toString(entity);
        } else {
            return null;
        }
    }

}
