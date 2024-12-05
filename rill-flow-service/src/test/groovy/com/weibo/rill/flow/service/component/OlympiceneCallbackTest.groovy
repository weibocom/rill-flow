package com.weibo.rill.flow.service.component

import com.weibo.rill.flow.interfaces.model.http.HttpParameter
import com.weibo.rill.flow.olympicene.core.model.dag.DAG
import com.weibo.rill.flow.olympicene.core.model.dag.DAGInfo
import com.weibo.rill.flow.olympicene.core.model.dag.DAGStatus
import com.weibo.rill.flow.olympicene.core.model.strategy.CallbackConfig
import com.weibo.rill.flow.olympicene.core.switcher.SwitcherManager
import com.weibo.rill.flow.olympicene.traversal.callback.DAGCallbackInfo
import com.weibo.rill.flow.olympicene.traversal.mappings.JSONPathInputOutputMapping
import com.weibo.rill.flow.service.invoke.HttpInvokeHelper
import com.weibo.rill.flow.service.statistic.TenantTaskStatistic
import com.weibo.rill.flow.service.storage.LongTermStorage
import org.springframework.http.HttpEntity
import org.springframework.http.HttpMethod
import spock.lang.Specification

import java.util.concurrent.ExecutorService

class OlympiceneCallbackTest extends Specification {
    OlympiceneCallback callback
    HttpInvokeHelper httpInvokeHelper = Mock(HttpInvokeHelper)
    JSONPathInputOutputMapping inputOutputMapping = Mock(JSONPathInputOutputMapping)
    LongTermStorage longTermStorage = Mock(LongTermStorage)
    ExecutorService callbackExecutor = Mock(ExecutorService)
    TenantTaskStatistic tenantTaskStatistic = Mock(TenantTaskStatistic)
    SwitcherManager switcherManagerImpl = Mock(SwitcherManager)

    def setup() {
        callback = new OlympiceneCallback(httpInvokeHelper, inputOutputMapping, longTermStorage, callbackExecutor, tenantTaskStatistic, switcherManagerImpl)
    }

    def "test buildRequestParams"() {
        given:
        DAGInfo dagInfo = Mock(DAGInfo)
        CallbackConfig callbackConfig = Mock(CallbackConfig)
        DAGCallbackInfo dagCallbackInfo = Mock(DAGCallbackInfo)
        dagCallbackInfo.getDagInfo() >> dagInfo
        dagCallbackInfo.getContext() >> ["key": "value"]
        callbackConfig.getInputMappings() >> []
        callbackConfig.getFullDAGInfo() >> false
        callbackConfig.getFullContext() >> false
        dagInfo.getExecutionId() >> "testExecutionId"
        dagInfo.getDagStatus() >> DAGStatus.SUCCEED
        httpInvokeHelper.buildRequestParams(*_) >> HttpParameter.builder().body(new HashMap<String, Object>()).build()

        when:
        HttpParameter result = callback.buildRequestParams(callbackConfig, dagCallbackInfo)

        then:
        result.getBody().get("execution_id") == "testExecutionId"
        result.getBody().get("dag_status") == DAGStatus.SUCCEED.getValue()
        result.getBody().get("data") == null
    }

    def "test flowCompletedCallback with null headers"() {
        given:
        DAGInfo dagInfo = Mock(DAGInfo)
        DAG dag = Mock(DAG)
        dagInfo.getDag() >> dag
        CallbackConfig callbackConfig = Mock(CallbackConfig)
        DAGCallbackInfo dagCallbackInfo = Mock(DAGCallbackInfo)
        dagCallbackInfo.getDagInfo() >> dagInfo
        dagCallbackInfo.getContext() >> ["key": "value"]
        callbackConfig.getResourceName() >> "http://test.url"
        callbackConfig.getInputMappings() >> []
        callbackConfig.getFullDAGInfo() >> false
        callbackConfig.getFullContext() >> false
        dag.getCallbackConfig() >> callbackConfig
        dagInfo.getExecutionId() >> "testExecutionId"
        dagInfo.getDagStatus() >> DAGStatus.SUCCEED
        httpInvokeHelper.buildRequestParams(*_) >> HttpParameter.builder().body(new HashMap<String, Object>()).build()
        httpInvokeHelper.buildUrl(*_) >> "http://test.url"

        when:
        callback.flowCompletedCallback(0, dagCallbackInfo)

        then:
        1 * httpInvokeHelper.invokeRequest("testExecutionId", null, "http://test.url", { HttpEntity<Map<String, Object>> entity ->
            entity.getHeaders().isEmpty()
        }, HttpMethod.POST, 1)
    }

    def "test flowCompletedCallback with non-null headers"() {
        given:
        DAGInfo dagInfo = Mock(DAGInfo)
        DAG dag = Mock(DAG)
        dagInfo.getDag() >> dag
        CallbackConfig callbackConfig = Mock(CallbackConfig)
        DAGCallbackInfo dagCallbackInfo = Mock(DAGCallbackInfo)
        dagCallbackInfo.getDagInfo() >> dagInfo
        dagCallbackInfo.getContext() >> ["key": "value"]
        dagInfo.getExecutionId() >> "testExecutionId"
        dagInfo.getDagStatus() >> DAGStatus.SUCCEED
        callbackConfig.getResourceName() >> "http://test.url"
        dag.getCallbackConfig() >> callbackConfig
        HttpParameter requestParams = Mock(HttpParameter)
        requestParams.getHeader() >> ["Authorization": "Bearer token"]
        requestParams.getBody() >> [:]
        httpInvokeHelper.buildRequestParams(*_) >> requestParams
        httpInvokeHelper.buildUrl(*_) >> "http://test.url"

        when:
        callback.flowCompletedCallback(0, dagCallbackInfo)

        then:
        1 * httpInvokeHelper.invokeRequest("testExecutionId", null, "http://test.url", { HttpEntity<Map<String, Object>> entity ->
            entity.getHeaders().get("Authorization") == ["Bearer token"]
        }, HttpMethod.POST, 1)
    }
}
