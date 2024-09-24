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

package com.weibo.api.flow.executors.utils;

import com.weibo.api.flow.executors.model.enums.AnswerExpressionMetaType;
import com.weibo.api.flow.executors.model.tasks.AnswerExpressionMeta;

import java.util.ArrayList;
import java.util.List;

public class ExpressionUtil {
    private ExpressionUtil() {}

    private static final String LEFT_TASK_TAG = "{{#";
    private static final String RIGHT_TASK_TAG = "#}}";
    private static final String LEFT_TASK_TAG_ESCAPED = "{{\\#";
    private static final String RIGHT_TASK_TAG_ESCAPED = "#\\}}";
    private static final String NORMAL_TASK_TEXT_TAG = "$.";

    public static List<AnswerExpressionMeta> parseAnswerTaskExpression(String expression) {
        if (expression == null) {
            return List.of();
        }

        List<AnswerExpressionMeta> result = new ArrayList<>();
        StringBuilder plainText = new StringBuilder();
        int i = 0;

        while (i < expression.length()) {
            if (!(i + LEFT_TASK_TAG.length() < expression.length() && expression.startsWith(LEFT_TASK_TAG, i))) {
                i = handleEscapeSequences(expression, plainText, i);
                continue;
            }
            addPlainTextIfNotEmpty(result, plainText);

            int endIndex = expression.indexOf(RIGHT_TASK_TAG, i + RIGHT_TASK_TAG.length());
            if (endIndex == -1) {
                plainText.append(expression.substring(i));
                break;
            }

            String taskContent = expression.substring(i + LEFT_TASK_TAG.length(), endIndex);
            int contextIndex = taskContent.indexOf(NORMAL_TASK_TEXT_TAG);

            result.add(createAnswerSessionMeta(taskContent, contextIndex));

            i = endIndex + RIGHT_TASK_TAG.length();
        }

        addPlainTextIfNotEmpty(result, plainText);

        return result;
    }

    private static void addPlainTextIfNotEmpty(List<AnswerExpressionMeta> result, StringBuilder plainText) {
        if (plainText.isEmpty()) {
            return;
        }
        result.add(AnswerExpressionMeta.builder()
                .type(AnswerExpressionMetaType.PLAIN)
                .text(plainText.toString())
                .build());
        plainText.setLength(0);
    }

    private static AnswerExpressionMeta createAnswerSessionMeta(String taskContent, int contextIndex) {
        if (contextIndex == -1) {
            return AnswerExpressionMeta.builder()
                    .type(AnswerExpressionMetaType.STREAM_TASK)
                    .taskName(taskContent)
                    .build();
        }
        return AnswerExpressionMeta.builder()
                .type(AnswerExpressionMetaType.FUNCTION_TASK)
                .taskName(taskContent.substring(0, contextIndex))
                .text(taskContent.substring(contextIndex))
                .build();
    }

    private static int handleEscapeSequences(String session, StringBuilder plainText, int i) {
        if (i + LEFT_TASK_TAG_ESCAPED.length() < session.length()) {
            if (session.startsWith(LEFT_TASK_TAG_ESCAPED, i)) {
                plainText.append(LEFT_TASK_TAG);
                return i + LEFT_TASK_TAG_ESCAPED.length();
            } else if (session.startsWith(RIGHT_TASK_TAG_ESCAPED, i)) {
                plainText.append(RIGHT_TASK_TAG);
                return i + RIGHT_TASK_TAG_ESCAPED.length();
            }
        }
        plainText.append(session.charAt(i));
        return i + 1;
    }
}
