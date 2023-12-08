package com.rill.flow.executor.wrapper;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @author yansheng3
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ExecutorContext {

    private String callbackUrl;
    private String mode;
    private Map<String, Object> body;
}
