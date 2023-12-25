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

package com.weibo.rill.flow.common.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * 采用自阿里云OSS存储sdk中的加密方式
 * copy from multimedia
 */
@Slf4j
public class HmacSHA1Signature {

    /* The default encoding. */
    private static final String DEFAULT_ENCODING = "UTF-8";

    /* Signature method. */
    private static final String ALGORITHM = "HmacSHA1";

    /* Signature version. */
    private static final String VERSION = "1";

    private static final Object LOCK = new Object();

    /* Prototype of the Mac instance. */
    private static volatile Mac macInstance;

    static {
        try {
            macInstance = Mac.getInstance(ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            log.error("no such algorithm! algorithm={}", ALGORITHM, e);
            macInstance = null;
        }
    }

    public String getAlgorithm() {
        return ALGORITHM;
    }

    public String getVersion() {
        return VERSION;
    }

    public String computeSignature(String key, String data) {
        try {
            byte[] signData = sign(key.getBytes(DEFAULT_ENCODING), data.getBytes(DEFAULT_ENCODING));
            return new String(Base64.encodeBase64(signData));
        } catch (UnsupportedEncodingException ex) {
            log.error("Unsupported algorithm: {}", ALGORITHM, ex);
            throw new RuntimeException("Unsupported algorithm: " + DEFAULT_ENCODING, ex);
        }
    }


    private byte[] sign(byte[] key, byte[] data) {
        try {
            // Because Mac.getInstance(String) calls a synchronized method, it could block on
            // invoked concurrently, so use prototype pattern to improve perf.
            if (macInstance == null) {
                synchronized (LOCK) {
                    if (macInstance == null) {
                        macInstance = Mac.getInstance(ALGORITHM);
                    }
                }
            }

            Mac mac = null;
            try {
                mac = (Mac) macInstance.clone();
            } catch (CloneNotSupportedException e) {
                log.warn("clone not support", e);
                mac = Mac.getInstance(ALGORITHM);
            }
            mac.init(new SecretKeySpec(key, ALGORITHM));
            return mac.doFinal(data);
        } catch (NoSuchAlgorithmException ex) {
            log.error("Unsupported algorithm: {}", ALGORITHM, ex);
            throw new RuntimeException("Unsupported algorithm: " + ALGORITHM, ex);
        } catch (InvalidKeyException ex) {
            log.error("Invalid key: {}", key, ex);
            throw new RuntimeException("Invalid key: " + Arrays.toString(key), ex);
        }
    }
}
