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

package com.weibo.rill.flow.interfaces.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicNameValuePair;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class WebHttpClientUtil {

    public static HttpPost httpPost(String url, Map<String, String> header, Map<String, Object> param, String body) throws UnsupportedEncodingException {
        HttpPost httpPost = new HttpPost(url);

        if (param != null && !param.isEmpty()) {
            List<NameValuePair> pairs = new ArrayList<>();
            for (Map.Entry<String, Object> entry : param.entrySet()) {
                pairs.add(new BasicNameValuePair(entry.getKey(), entry.getValue().toString()));
            }
            httpPost.setEntity(new UrlEncodedFormEntity(pairs));
        }

        if (header != null && !header.isEmpty()) {
            for (Map.Entry<String, String> headerEntry : header.entrySet()) {
                httpPost.addHeader(headerEntry.getKey(), headerEntry.getValue());
            }
        }

        if (StringUtils.isNotBlank(body)) {
            httpPost.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));
        }

        return httpPost;
    }

    public static HttpGet httpGet(String url, Map<String, Object> param, Map<String, String> header) {
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

        return httpGet;
    }

}
