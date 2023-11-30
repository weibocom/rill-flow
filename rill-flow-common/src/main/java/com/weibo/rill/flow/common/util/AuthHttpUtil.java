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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.weibo.rill.flow.common.util;

import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Map;

/**
 * copy from multimedia
 */
public class AuthHttpUtil {

    private AuthHttpUtil() {
    }

    /**
     * Encode a URL segment with special chars replaced.
     */
    public static String urlEncode(String value, String encoding) {
        if (value == null) {
            return "";
        }
        try {
            String encoded = URLEncoder.encode(value, encoding);
            return encoded.replace("+", "%20").replace("*", "%2A")
                    .replace("~", "%7E").replace("/", "%2F");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("FailedToEncodeUri", e);
        }
    }

    public static String urlDecode(String value, String encoding) {
        if (StringUtils.isBlank(value)) {
            return value;
        }
        try {
            return URLDecoder.decode(value, encoding);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("FailedToDecodeUrl", e);
        }
    }

    /**
     * Encode request parameters to URL segment.
     */
    public static String paramToQueryString(Map<String, String> params, String charset) {
        if (params == null || params.isEmpty()) {
            return null;
        }

        return params.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(p -> urlEncode(p.getKey(), charset) + "=" + urlEncode(p.getValue(), charset))
                .reduce((p1, p2) -> p1 + "&" + p2)
                .orElse("");
    }

    public static void addSignToParam(Map<String, String> params, String key) {
        if (params == null || params.isEmpty()) {
            return;
        }

        String sign = calculateSign(params, key);
        if (StringUtils.isNotBlank(sign)) {
            params.put("sign", sign);
        }
    }

    public static String calculateSign(Map<String, String> params, String key) {
        if (params == null || params.isEmpty()) {
            return null;
        }

        String paramStr = paramToQueryString(params, "utf-8");
        if (StringUtils.isNotBlank(paramStr)) {
            return new HmacSHA1Signature().computeSignature(key, paramStr);
        }
        return null;
    }
}
