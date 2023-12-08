package com.rill.flow.executor.wrapper;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * @author yansheng3
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CallbackData implements Serializable {
    /**
     * callback url
     */
    @JSONField(serialize = false)
    private String url;
    @JSONField(name = "result_type")
    private String resultType;
    private Map<String, Object> result;
}
