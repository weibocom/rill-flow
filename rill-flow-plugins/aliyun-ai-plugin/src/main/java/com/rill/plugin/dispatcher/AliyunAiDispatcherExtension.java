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

import com.alibaba.dashscope.aigc.conversation.Conversation;
import com.alibaba.dashscope.aigc.generation.Generation;
import com.alibaba.dashscope.aigc.generation.GenerationParam;
import com.alibaba.dashscope.aigc.generation.GenerationResult;
import com.alibaba.dashscope.exception.ApiException;
import com.alibaba.dashscope.exception.InputRequiredException;
import com.alibaba.dashscope.exception.NoApiKeyException;
import com.alibaba.dashscope.utils.JsonUtils;
import com.weibo.rill.flow.interfaces.dispatcher.DispatcherExtension;
import com.weibo.rill.flow.interfaces.model.http.HttpParameter;
import com.weibo.rill.flow.interfaces.model.resource.Resource;
import com.weibo.rill.flow.interfaces.model.strategy.DispatchInfo;
import com.weibo.rill.flow.interfaces.model.task.TaskInfo;
import com.weibo.rill.flow.interfaces.utils.HttpUtil;
import org.pf4j.Extension;

import java.util.Map;
import java.util.Optional;

@Extension
public class AliyunAiDispatcherExtension implements DispatcherExtension {

    @Override
    public String handle(Resource resource, DispatchInfo dispatchInfo) {
        TaskInfo taskInfo = dispatchInfo.getTaskInfo();
        Map<String, Object> input = dispatchInfo.getInput();
        if (input == null || input.isEmpty()) {
            throw new RuntimeException("input cannot be empty");
        }

        String apikey = input.get("apikey").toString();
        String model = Optional.ofNullable(input.get("model")).map(Object::toString).orElse(Conversation.Models.QWEN_PLUS);

        String executionId = dispatchInfo.getExecutionId();
        String taskInfoName = taskInfo.getName();

        HttpParameter requestParams = HttpUtil.functionRequestParams(executionId, taskInfoName, input);

        try {
            Map<String, Object> body = requestParams.getBody();
            String prompt = String.valueOf(body.get("message"));
            if (body.get("message_prefix") != null) {
                prompt = body.get("message_prefix") + prompt;
            }
            Object messageSuffix = body.get("message_suffix");
            if (messageSuffix != null) {
                prompt = prompt + messageSuffix;
            }

            GenerationResult result = tokenizer(prompt, apikey,model);

            return JsonUtils.toJson(result);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public String getName() {
        return "aliyun_ai";
    }

    @Override
    public String getIcon() {
        return "iVBORw0KGgoAAAANSUhEUgAAAEAAAABACAYAAACqaXHeAAAACXBIWXMAAAsTAAALEwEAmpwYAAAHLklEQVR4nO1beVCTRxT/EpLsJh0" +
                "dizOtvf6o05m2tp1pi7KbIMUoIqCgglA8OLIbEG/Ucaxai0eVUoo6tlq1KmqrnXZsrVanahWtF6hQ8WK8UHGsR/FqvWassp3d5AsEM" +
                "SSQhID5zbxx8h3s/n7f27dv366S5IcffvjhRzMgISHgGUSfB51NHdtiGii1dmj0GW9ARMZBRDYARK5CTJm9kbsAkxKI6TyATeFcIKk" +
                "1QINoNMRkK8S02o5wiJmBmCwGeo1g8IOhdcSgDCBSCRBJl4Iy1FJLBOyc9grEZJONVLdMpp74GQvYsJYpyncx6VoZk24etpniwn6m3" +
                "LKeqeZ8zUDfsTViIHJag2mM1JKgRSQeYvqPINBjGFMtWsqkyyV2hB3a9TIW8OtPTJMyWRaiGiAyu0UMCw2ioyEmj3jH1RNymKKy2Hn" +
                "i9QihWryMwdAM2Rs2SzhBK/kqACaZoqMGM1N9U8CkG/Zu3lhTHvydgYgRltiA6VpJylZKvgaAaDeIyQPeyYDVq91C3C5GlO1gIGqUL" +
                "EK+5FMwkDYA0b9451TzF7mdvM0TirYwGJJuESGYGCVfAcA0n3dKkzJJjFtPCcBNxARLYDzuE1OkNtj8snB9g5kpS7d7lLwcGEHiBHk" +
                "ojPSFwDdDRPzJn3uevNUC1v0ozwpnmi8ghqW20yASCzC9zDuj3LnJawIIL4jJsopg6upF0tkqiEgSQHQfRPShLW2NG++2Kc9ZU8+cJ" +
                "6fMuV7hrtGb+0BMztZObTVDs0Xqqjiyw6vkuSl/W2eNA2SXZ5njBC3EZLHtaw+cyAK+X8OkK6VeJ13bFKf3yh5ww3PkgzJ0ENFtgnz" +
                "YUKZavsLj05zTVnWIQb1ZiKAJNnXitQVJkhTuJK+GmGwXKkePFplYs5OuYyBipP0yGpH/ACL7ISaTdV1SOzSJP8AkR5DvM8ayfPUBw" +
                "nVNPTFX1BJ4H0HvMTaPsNodgOg0HrhdJg+DzaFiRceTm32bm52o03a1lCm3rmfq8bNrxEB0m8slN4DpbpHXf7Wk+Uk10pQ7NjIQM0Y" +
                "OlHulyFHAua+PTF3FSxEjXCti+KApTu6xiQAxXeCcAJgsb+lf384TSreL+iNP3NR68nbD7o/oRS6A4pAXFjZeMnXOfHnxtMoheY2Bv" +
                "C4e7D2m2TvtTlMc3WmNBfSmwyU0QLSnSCyGTXepAV4BEm72WK3fQxZiZgFrXKs6gQTLElqH095/8vhHdIhY2k7Kc83FpuZ7j7zVeJu" +
                "u9FEzeqZ4T4tN/R14AEkXf3zG3EYJsHJ5LKs+bfSo8TYaI4A6e44cB8xPjgGIxIohkDWr1QmgGTvLsm5wtMmiQ+YgoVLihFYngFxGc" +
                "xgDpKAMNV9eimnQhfzfFQHuHjc+3LUxsrKqtPu9+u7fK+9efaG45213CqA4sVvOCK83uDYAiH5nKW8v9ogA78Yl8x1gpjPQv++XG6v" +
                "r3n+rb/Jhfr9gaUy5uwRQfblELp58KzUELabBYie3WyZTnN3nVgFuHzM+1OrJPTmSr1vT+0TdZ16NSj3O7+XN7VfmDgEU54oZ7D7MM" +
                "nsYiL5BATj49pMIGMOnP7aL2xQBViyLPWoZh+Q2/zeKDCr2qADXy2zTn2VLzUloccpLAJNLoqFP8hsUwVkBug4evJ8/lzU1vox7WZs" +
                "Q06VHpzwkwLUypp4+V3b9S5yT0wJwaJEJQUzvC0/IzBY1uKYI8PCkkekMpIrXGa6UhN9pH0Yq+DsHNkdedLcAiooikc1a6wH3nXb9u" +
                "tDq07vIiyNeeVF9sZApygobJcC2n6ME4Q7GtJP8d9zwpCL+2zT+wyJ3CcCr06o5C0X90vblg81Yagp0QRkvAERW1z7iAmKzhFeop+S" +
                "JrBEMGN+gAPFWwpFkUMmp3RG35s3vW85/P2c0nWqMALxN3jbvA//adqdKxIEK+oPWQF5sEvl6kqQl8k5QfeZIgMBQU+WT3qvY0/OWq" +
                "wLUZwCTKxDRpY6TnSYjW8lPfIFgc3cYTJIBMmeInSIHAhzbGVFlWYjQfxNGJhXJ1j6MnOHXp8yMO+iyByC6j7ct+qAnPTSIvtls+4Q" +
                "Q0wJHAoyeMqCY33+nf3JJ7esf58Qf4Ndf6516uBEeUCD5CmADAqxcFnMkMMx0tmBpzJHa1yuLw28GhqVVpI5LtOUDQ8YmHnw21HT+j" +
                "w2R51uNANVuNL8Ay/0ewHx2CLwXn8Ki6SCb9cscyI7t6FWvKx8p7MWSRiXZPe+M8TZ8TgCASO6T5uZP8+LsiPPcf8HCvqxdKGlSTdB" +
                "rByOcQqcEDcSmMH7Ku8boKt7RGbk1AlT9Gc7ihw2sRYQstH/HOeNt8TYlXwZAdFptAQp/iWYdo9LkjK2K1x+l1gxgFSA7J06IoDPYX" +
                "L7Q5aVpSxagbVebuz+AiH7kk+d8PSmA1c5BfbpBepoAMRlsXbSs5GeJpacRbZ+G/wzlhx9++CG1YPwPxbmnePNcQgEAAAAASUVORK5" +
                "CYII=";
    }

    @Override
    public String getSchema() {
        return "{\"type\":\"object\",\"properties\":{\"apikey\":{\"type\":\"string\",\"description\":\"apikey\"},\"required\":[\"apikey\"]}}";
    }

    private GenerationResult tokenizer(String prompt, String apikey, String model) throws ApiException, NoApiKeyException, InputRequiredException {
        Generation generation = new Generation();
        GenerationParam param = GenerationParam.builder()
                .model(model)
                .prompt(prompt)
                .apiKey(apikey)
                .build();
        return generation.call(param);
    }
}
