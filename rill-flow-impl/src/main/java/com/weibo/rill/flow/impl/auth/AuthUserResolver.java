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

package com.weibo.rill.flow.impl.auth;

import com.weibo.rill.flow.common.model.User;
import com.weibo.rill.flow.impl.constant.FlowErrorCode;
import com.weibo.rill.flow.impl.model.FlowUser;
import com.weibo.rill.flow.olympicene.core.exception.AuthException;
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager;
import com.weibo.rill.flow.common.util.AuthHttpUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.MethodParameter;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static java.util.stream.Collectors.toMap;

@Slf4j
public class AuthUserResolver implements HandlerMethodArgumentResolver {

    private static final String SIGN = "sign";

    private static final String TS = "ts";

    private static final long ONE_DAY_MS = 24 * 60 * 60 * 1000L;

    @Value("${rill_flow_auth_secret_key}")
    private String authSecret;

    @Autowired
    private SwitcherManager switcherManager;

    @Override
    public boolean supportsParameter(MethodParameter parameter) {
        return parameter.getParameterType() == User.class;
    }

    @Override
    public Object resolveArgument(@Nullable MethodParameter methodParameter, ModelAndViewContainer mavContainer,
                                  @NotNull NativeWebRequest nativeWebRequest, WebDataBinderFactory binderFactory) throws Exception {

        if (!switcherManager.getSwitcherState("ENABLE_AUTH_RESOLVER")) {
            FlowUser flowUser = new FlowUser();
            flowUser.setUid(127001L);
            return flowUser;
        }
        HttpServletRequest request = nativeWebRequest.getNativeRequest(HttpServletRequest.class);
        if (Objects.isNull(request)) {
            throw new AuthException(FlowErrorCode.AUTH_FAILED.getCode(), FlowErrorCode.AUTH_FAILED.getMessage());
        }
        Map<String, String[]> requestParamsMap = request.getParameterMap();
        Map<String, String> paramsMap = requestParamsMap.entrySet()
                .stream()
                .collect(toMap(Map.Entry::getKey, stringEntry -> String.join(",", stringEntry.getValue())));

        if (MapUtils.isEmpty(paramsMap)) {
            throw new AuthException(FlowErrorCode.AUTH_FAILED.getCode(), FlowErrorCode.AUTH_FAILED.getMessage());
        }
        if (!paramsMap.containsKey(SIGN)) {
            throw new AuthException(FlowErrorCode.NO_SIGN.getCode(), FlowErrorCode.NO_SIGN.getMessage());
        }
        try {
            Long clientTimestamp = NumberUtils.toLong(paramsMap.get(TS));
            Long serverTimestamp = System.currentTimeMillis();
            if (serverTimestamp - clientTimestamp > ONE_DAY_MS) {
                throw new AuthException(FlowErrorCode.AUTH_EXPIRED.getCode(), FlowErrorCode.AUTH_EXPIRED.getMessage());
            }

            String clientSign = paramsMap.get(SIGN);
            paramsMap.remove(SIGN);
            String serverSign = AuthHttpUtil.calculateSign(new TreeMap<>(paramsMap), authSecret);
            if (!StringUtils.equals(clientSign, serverSign)) {
                log.warn("auth failed, invalid sign, clientSign:{}, serverSign:{}, paramMap:{}", clientSign, serverSign, requestParamsMap);
                throw new AuthException(FlowErrorCode.AUTH_FAILED.getCode(), FlowErrorCode.AUTH_FAILED.getMessage());
            }

            FlowUser flowUser = new FlowUser();
            flowUser.setUid(0L);
            return flowUser;
        } catch (Exception e) {
            log.error("exception occur when do auth of upload", e);
        }
        throw new AuthException(FlowErrorCode.AUTH_FAILED.getCode(), FlowErrorCode.AUTH_FAILED.getMessage());
    }
}
