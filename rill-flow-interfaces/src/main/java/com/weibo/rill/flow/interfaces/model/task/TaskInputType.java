package com.weibo.rill.flow.interfaces.model.task;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum TaskInputType {
    BLOCK("block"),
    STREAM("stream");

    private final String value;

    public static TaskInputType getInputTypeByValue(String value) {
        if (value != null && value.equalsIgnoreCase("stream")) {
            return STREAM;
        }
        return BLOCK;
    }
}
