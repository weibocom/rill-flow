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

package com.weibo.rill.flow.impl.http;

import com.weibo.rill.flow.interfaces.http.FlowHttpClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class FlowWebHttpClient implements FlowHttpClient {

    private static final CloseableHttpClient httpClient = HttpClients.createDefault();

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
            HttpPost httpPost = new HttpPost(url);
            if (param!= null &&!param.isEmpty()) {
                List<NameValuePair> nvps = new ArrayList<>();
                for (Map.Entry<String, Object> entry : param.entrySet()) {
                    nvps.add(new BasicNameValuePair(entry.getKey(), entry.getValue().toString()));
                }
                httpPost.setEntity(new UrlEncodedFormEntity(nvps));
            }
            if (header!= null &&!header.isEmpty()) {
                for (Map.Entry<String, String> headerEntry: header.entrySet()) {
                    httpPost.addHeader(headerEntry.getKey(), headerEntry.getValue());
                }
            }
            httpPost.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));
            CloseableHttpResponse response = httpClient.execute(httpPost);
            return response.toString();
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
        StringBuilder reqUrl = new StringBuilder(url);
        if (param != null && !param.isEmpty()) {
            StringBuilder params = new StringBuilder();
            for (Map.Entry<String, Object> entry : param.entrySet()) {
                params.append("&").append(entry.getKey()).append("=").append(entry.getValue());
            }
            String paramConnector = "?";
            if (!url.contains(paramConnector)) {
                reqUrl.append(paramConnector);
                reqUrl.append(params.substring(1));
            } else {
                reqUrl.append(params);
            }
        }
        HttpGet httpGet = new HttpGet(reqUrl.toString());
        if (header != null && !header.isEmpty()) {
            for (Map.Entry<String, String> headerEntry: header.entrySet()) {
                httpGet.addHeader(headerEntry.getKey(), headerEntry.getValue());
            }
        }
        CloseableHttpResponse response = httpClient.execute(httpGet);
        return response.toString();
    }

    private String executePostRequest(String url, Map<String, Object> param, Map<String, String> header) throws IOException {
        HttpPost httpPost = new HttpPost(url);
        if (param!= null &&!param.isEmpty()) {
            List<NameValuePair> nvps = new ArrayList<>();
            for (Map.Entry<String, Object> entry : param.entrySet()) {
                nvps.add(new BasicNameValuePair(entry.getKey(), entry.getValue().toString()));
            }
            httpPost.setEntity(new UrlEncodedFormEntity(nvps));
        }
        if (header!= null &&!header.isEmpty()) {
            for (Map.Entry<String, String> headerEntry: header.entrySet()) {
                httpPost.addHeader(headerEntry.getKey(), headerEntry.getValue());
            }
        }
        CloseableHttpResponse response = httpClient.execute(httpPost);
        return response.toString();
    }
}
