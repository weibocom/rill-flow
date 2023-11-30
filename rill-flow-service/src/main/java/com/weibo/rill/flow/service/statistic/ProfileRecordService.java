package com.weibo.rill.flow.service.statistic;

import com.weibo.rill.flow.service.util.ProfileActions;
import com.weibo.rill.flow.service.util.PrometheusActions;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.function.Supplier;

@Service
public class ProfileRecordService {
    /**
     * 按业务类型分别统计 接口调用情况 运维配置的监控为接口总体调用情况
     */
    public Map<String, Object> runNotifyAndRecordProfile(String url, String id, Supplier<Map<String, Object>> notifyActions) {
        long startTime = System.currentTimeMillis();
        try {
            Map<String, Object> ret = notifyActions.get();
            ProfileActions.recordHttpExecution(url, id, true, System.currentTimeMillis() - startTime);
            // 记录prometheus
            PrometheusActions.recordHttpExecution(url, id, true, System.currentTimeMillis() - startTime);
            return ret;
        } catch (Exception e) {
            ProfileActions.recordHttpExecution(url, id, false, System.currentTimeMillis() - startTime);
            // 记录prometheus
            PrometheusActions.recordHttpExecution(url, id, false, System.currentTimeMillis() - startTime);
            throw e;
        }
    }
}
