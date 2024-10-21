package com.weibo.rill.flow.interfaces.model.task;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum TaskInputOutputType {
    BLOCK("block"),
    STREAM("stream");

    private final String value;

    public static TaskInputOutputType getTypeByValue(String value) {
        if (value != null && value.equalsIgnoreCase("stream")) {
            return STREAM;
        }
        return BLOCK;
    }
}
