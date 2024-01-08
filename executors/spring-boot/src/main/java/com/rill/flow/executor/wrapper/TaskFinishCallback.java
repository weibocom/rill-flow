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

package com.rill.flow.executor.wrapper;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.springframework.util.MimeTypeUtils.APPLICATION_JSON_VALUE;

/**
 * @author yansheng3
 */
@Service
@Slf4j
public class TaskFinishCallback {
    private static final CloseableHttpClient DEFAULT_HTTP_CLIENT = HttpClients.createDefault();

    public void onCompletion(CallbackData callbackData) {

        String requestBody = JSONObject.toJSONString(callbackData.getResult());
        Map<String, String> headers = Map.of("Content-Type", APPLICATION_JSON_VALUE);

        try {
            String res = postWithBody(callbackData.getUrl(), headers, null, requestBody);
            if (res == null) {
                log.error("callback is failed. url:{}, callback_data:{}, callback response is empty.", callbackData.getUrl(), requestBody);
            } else {
                log.info("callback is success.url:{}, request_body:{}, response:{}", callbackData.getUrl(), requestBody, res);
            }
        } catch (Exception e) {
            log.error("callback is failed. url:{}, callback_data:{}, error:{}", callbackData.getUrl(), callbackData, e.getMessage());
        }
    }

    private String postWithBody(String url, Map<String, String> header, Map<String, Object> param, String body) {
        try {
            HttpPost httpPost = new HttpPost(url);
            if (param != null && !param.isEmpty()) {
                List<NameValuePair> nvps = new ArrayList<>();
                for (Map.Entry<String, Object> entry : param.entrySet()) {
                    nvps.add(new BasicNameValuePair(entry.getKey(), entry.getValue().toString()));
                }
                httpPost.setEntity(new UrlEncodedFormEntity(nvps));
            }
            if (header != null && !header.isEmpty()) {
                for (Map.Entry<String, String> headerEntry : header.entrySet()) {
                    httpPost.addHeader(headerEntry.getKey(), headerEntry.getValue());
                }
            }
            httpPost.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));
            CloseableHttpResponse response = DEFAULT_HTTP_CLIENT.execute(httpPost);

            HttpEntity entity = response.getEntity();

            if (entity != null) {
                return EntityUtils.toString(entity);
            }
        } catch (Exception e) {
            log.error("http post with body error. url:{}, header:{}, param:{}, body:{}", url, header, param, body, e);
        }
        return null;
    }
}
