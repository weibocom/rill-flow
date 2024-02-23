package com.weibo.rill.flow.sample

import net.sf.json.groovy.JsonSlurper
import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import spock.lang.Specification
import spock.lang.Stepwise
import spock.lang.Timeout

@Stepwise
class SampleApiTest extends Specification {

    
    
    String domain;
    
    def setup(){
        domain = System.getProperty("api.url")
    }

    @Timeout(30)

    def "run choice sample task"() {
        when:
        def responseJson = sendPostRequest(domain + "/flow/bg/manage/descriptor/add_descriptor.json?business_id=rillFlowSample&feature_name=choiceSample&alias=release", "text/plain", readFileContent("../docs/samples/choice-sample.yaml"))

        then:
        responseJson.status == 200
        responseJson.content.ret == true

        when:
        def submitResponseJson = sendPostRequest(domain + "/flow/submit.json?descriptor_id=rillFlowSample:choiceSample", "application/json", "{\"input_num\":10}")

        then:
        submitResponseJson.status == 200
        submitResponseJson.content.execution_id != ""

        expect:
        assert checkDagStatus(submitResponseJson.content.execution_id)
    }

    def "run call api sample task"() {
        when:
        def responseJson = sendPostRequest(domain + "/flow/bg/manage/descriptor/add_descriptor.json?business_id=rillFlowSample&feature_name=callApiSample&alias=release", "text/plain", readFileContent("../docs/samples/call-api-sample.yaml"))

        then:
        responseJson.status == 200
        responseJson.content.ret == true

        when:
        def submitResponseJson = sendPostRequest(domain + "/flow/submit.json?descriptor_id=rillFlowSample:callApiSample", "application/json", "{\"input_num\":10}")

        then:
        submitResponseJson.status == 200
        submitResponseJson.content.execution_id != ""

        expect:
        assert checkDagStatus(submitResponseJson.content.execution_id)

    }

    private String readFileContent(String filePath) {
        try {
            return new File(filePath).text
        } catch (IOException e) {
            e.printStackTrace()
            return null
        }
    }

    private boolean checkDagStatus(String executionId) {
        def i = 0
        while (i < 10) {
            def getResponseJson = sendGetRequest(domain + "/flow/get.json?execution_id=" + executionId, "application/json")
            if (getResponseJson.content.ret.dag_status == "SUCCEED") {
                return true
            }
            i++
            println getResponseJson.content.ret.dag_status
            if (getResponseJson.content.ret.dag_status == "FAILED") {
                println getResponseJson.content.ret
            }
            Thread.sleep(1000)
        }
        return false
    }

    private Map<String, Object> sendPostRequest(String url, String contentType, String requestData) {
        HttpClient httpClient = HttpClientBuilder.create().build()
        def result = [:]
        try {
            HttpPost httpPost = new HttpPost(url)
            httpPost.addHeader("Content-Type", contentType)

            httpPost.setEntity(new StringEntity(requestData, ContentType.create(contentType)))

            HttpResponse response = httpClient.execute(httpPost)
            HttpEntity entity = response.getEntity()

            int statusCode = response.getStatusLine().getStatusCode()
            def responseContent = new JsonSlurper().parse(entity.content)
            result.status = statusCode
            result.content = responseContent
            result as Map<String, Object>
        } catch (Exception e) {
            e.printStackTrace()
            return null
        }
    }

    private Map<String, Object> sendGetRequest(String url, String contentType) {
        HttpClient httpClient = HttpClientBuilder.create().build()
        def result = [:]
        try {
            HttpGet httpGet = new HttpGet(url)
            httpGet.addHeader("Content-Type", contentType)

            HttpResponse response = httpClient.execute(httpGet)
            HttpEntity entity = response.getEntity()

            int statusCode = response.getStatusLine().getStatusCode()
            def responseContent = new JsonSlurper().parse(entity.content)
            result.status = statusCode
            result.content = responseContent
            result as Map<String, Object>
        } catch (Exception e) {
            e.printStackTrace()
            return null
        }
    }
}