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

package com.rill.plugin.dispatcher;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableMap;
import com.weibo.rill.flow.interfaces.dispatcher.DispatcherExtension;
import com.weibo.rill.flow.interfaces.executor.ThreadPoolHolder;
import com.weibo.rill.flow.interfaces.http.RillFlowWebHttpClient;
import com.weibo.rill.flow.interfaces.model.http.HttpParameter;
import com.weibo.rill.flow.interfaces.model.resource.Resource;
import com.weibo.rill.flow.interfaces.model.strategy.DispatchInfo;
import com.weibo.rill.flow.interfaces.model.task.FunctionTask;
import com.weibo.rill.flow.interfaces.utils.HttpUtil;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.pf4j.Extension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import static com.weibo.rill.flow.interfaces.model.task.FunctionPattern.TASK_ASYNC;


@Extension
public class ChatGPTDispatcherExtension implements DispatcherExtension {
    private static final Logger logger = LoggerFactory.getLogger(ChatGPTDispatcherExtension.class);

    private static final RillFlowWebHttpClient FLOW_WEB_HTTP_CLIENT = new RillFlowWebHttpClient();
    private static final String OPENAI_URL = "https://api.openai.com/v1/chat/completions";

    private static final ThreadPoolExecutor THREAD_POOL_EXECUTOR = ThreadPoolHolder.getThreadPoolExecutor();

    @Override
    public String handle(Resource resource, DispatchInfo dispatchInfo) {

        Map<String, Object> input = dispatchInfo.getInput();

        FunctionTask functionTask = (FunctionTask) dispatchInfo.getTaskInfo().getTask();

        if (MapUtils.isEmpty(input)) {
            throw new RuntimeException("parameters is empty");
        }

        try {

            if (StringUtils.isBlank((String) input.get("apikey"))) {
                throw new RuntimeException("apikey is empty");
            }

            if (TASK_ASYNC.equals(functionTask.getPattern())) {
                THREAD_POOL_EXECUTOR.submit(() -> asyncExecute(dispatchInfo));
                return "{\"result\": {\"err_msg\": \"success\"}}";
            } else {
                return execute(dispatchInfo);
            }

        } catch (Exception e) {
            logger.error("requesting chatgpt is failed. url:{}, error:{}", OPENAI_URL, e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public String getName() {
        return "chatgpt";
    }

    @Override
    public String getIcon() {
        return "iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAMAAAAoLQ9TAAAABGdBTUEAALGPC/xhBQAAAAFzUkdCAK7OHOkAAAEgUExURXWpnHWpnHSq" +
                "nHWpm3OsmXWqnHWpnHaqnXWpnExpcXWpnOHs6XapnIq2q3aqneLt6vH39dLj38fc19Tk4Ie0qYi1qpO8sbDOxpW9s93q54Wzp3utoOT" +
                "u7NDi3Ya0qH+vo9Pj38re2XeqnbLPx7HOx6DEu5O8snqsoNzp5uHs6tro5JG6sMHZ09vp5dbm4sPa1Iu2rL7W0JG7sJC6r4+5r7PPyK" +
                "HEu3irntjn436uovD29eDs6e308qPGvc7h3Nnn5MLZ05K7sbvVzuPu6/H29d/r6M3g28/h3NLj3n2uoYm1qpS8snmsn3ytoXeqnrjSz" +
                "Nbl4anKwnutoaTGvZzBuK/Nxs7g3M/h3cbb1qvKwsjd19Hi3tDi3tXl4Zm/tq7NxZ7VYGwAAAAKdFJOU/Lxra4osK8n7wDKnPAhAAAA" +
                "CXBIWXMAAAA7AAAAOwG4ag7yAAAAyklEQVQY02PgZGFl4IICBjZ2TgYWDi4kwMHOwAqizbitLXjVQSwmBpB6MXvfIDcbSQ1eLi5GEN9" +
                "UwDtYJtCTz4BfCWgOF5e4g6BcmHyorKuEvhpYgN9HVk5YMSZERFDACCzg7BfOK8TDoyDkJW4IFnDyF4mI5eOJEvHg4wcLmLgE8MYpKA" +
                "pLCZpzgwW0hRwlo+Pl3aWEdVTAAjzcxjzSkTK2EgLKIOcDRUS1uK3spC11xSD+AZF8onrcmqoQD7KjeA4owMnOzIgsAACgVhkks/FNt" +
                "AAAAFd6VFh0UmF3IHByb2ZpbGUgdHlwZSBpcHRjAAB4nOPyDAhxVigoyk/LzEnlUgADIwsuYwsTIxNLkxQDEyBEgDTDZAMjs1Qgy9jU" +
                "yMTMxBzEB8uASKBKLgDqFxF08kI1lQAAAABJRU5ErkJggg==";
    }

    private void asyncExecute(DispatchInfo dispatchInfo) {
        String resultTye = "SUCCESS";
        String result = null;
        try {
            result = execute(dispatchInfo);
        } catch (Exception e) {
            resultTye = "FAILED";
            logger.error("execute is failed. error:{}", e.getMessage());
        } finally {
            callback(dispatchInfo, result, resultTye);
        }
    }

    private void callback(DispatchInfo dispatchInfo, String result, String resultType) {
        logger.info("callback is start");
        MultiValueMap<String, String> headers = dispatchInfo.getHeaders();
        String callbackUrl = headers.get("X-Callback-Url").stream().findFirst().get();
        try {
            JSONObject jsonObject;
            if (StringUtils.isNotBlank(result)) {
                jsonObject = JSON.parseObject(result);
            } else {
                jsonObject = new JSONObject();
            }

            String callbackBody = JSON.toJSONString(ImmutableMap.of("result_type", resultType, "result", jsonObject));
            String callbackResponse = FLOW_WEB_HTTP_CLIENT.postWithBody(callbackUrl, null, null, callbackBody, null);
            logger.info("callback rill flow is success. callback url:{}, body:{}, response:{}", callbackUrl, callbackBody, callbackResponse);
        } catch (Exception e) {
            logger.error("callback rill flow is failed. callbackUrl:{}, error:{}", callbackUrl, e.getMessage());
        } finally {
            logger.info("callback is end");
        }
    }

    private String execute(DispatchInfo dispatchInfo) {
        Map<String, Object> input = dispatchInfo.getInput();

        String apiKey = (String) input.get("apikey");
        Map<String, String> header = new HashMap<>();
        header.put("Authorization", "Bearer " + apiKey);
        header.put("Content-Type", "application/json");

        HttpParameter httpParameter = HttpUtil.functionRequestParams(dispatchInfo);
        String promptPrefix = (String) httpParameter.getBody().get("prompt_prefix");
        String promptSuffix = (String) httpParameter.getBody().get("prompt_suffix");
        String prompt = (String) httpParameter.getBody().get("prompt");

        if (StringUtils.isBlank(prompt)) {
            throw new RuntimeException("prompt is empty");
        }
        if (promptPrefix != null) {
            prompt = promptPrefix + prompt;
        }
        if (promptSuffix != null) {
            prompt = prompt + promptSuffix;
        }

        String model = (String) input.getOrDefault("model", "gpt-3.5-turbo");

        String body = "{\"model\": \"" + model + "\", \"messages\": [{\"role\": \"user\", \"content\": \"" + prompt + "\"}]}";
        String result = FLOW_WEB_HTTP_CLIENT.postWithBody(OPENAI_URL, header, null, body, null);

        logger.info("request chatgpt is success. request body:{}, response:{}", body, result);
        return result;

    }

}
