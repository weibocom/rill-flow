package com.weibo.rill.flow.sample

import com.alibaba.fastjson.JSONObject
import net.sf.json.groovy.JsonSlurper
import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import spock.lang.Specification
import spock.lang.Stepwise

@Stepwise
class SampleApiTest extends Specification {


    def "choice sample add dag task"() {
        when:
        String url = "http://localhost:8080/flow/bg/manage/descriptor/add_descriptor.json?business_id=rillFlowSample&feature_name=choiceSample&alias=release";
        String contentType = "text/plain";
        String requestData = readFileContent("../docs/samples/choice-sample.yaml");

        then:
        JSONObject responseJson = sendApiRequest(url, contentType, requestData);

        expect:
        responseJson.statusCode == 200
        responseJson.getJSONObject("responseContent").getBoolean("ret") == true

    }

    def "choice sample submit dag task"() {
        when:
        String url = "http://localhost:8080/flow/submit.json?descriptor_id=rillFlowSample:choiceSample";
        String contentType = "application/json";
        String requestData = "{\"input_num\":10}";

        then:
        JSONObject responseJson = sendApiRequest(url, contentType, requestData);

        expect:
            responseJson.statusCode == 200
            responseJson.getJSONObject("responseContent").getString("execution_id") != ""
    }

    def "call api sample add dag task"() {
        when:
        String url = "http://localhost:8080/flow/bg/manage/descriptor/add_descriptor.json?business_id=rillFlowSample&feature_name=callApiSample&alias=release";
        String contentType = "text/plain";
        String requestData = readFileContent("../docs/samples/call-api-sample.yaml");

        then:
        JSONObject responseJson = sendApiRequest(url, contentType, requestData);

        expect:
        responseJson.statusCode == 200
        responseJson.getJSONObject("responseContent").getBoolean("ret") == true
    }

    def "call api sample submit dag task"() {
        when:
        String url = "http://localhost:8080/flow/submit.json?descriptor_id=rillFlowSample:callApiSample";
        String contentType = "application/json";
        String requestData = "{\"input_num\":10}";

        then:
        JSONObject responseJson = sendApiRequest(url, contentType, requestData);

        expect:
        responseJson.statusCode == 200
        responseJson.getJSONObject("responseContent").getString("execution_id") != ""
    }

    def "parallel async sample add dag task"() {
        when:
        String url = "http://localhost:8080/flow/bg/manage/descriptor/add_descriptor.json?business_id=rillFlowSample&feature_name=parallelAsyncTask&alias=release";
        String contentType = "text/plain";
        String requestData = readFileContent("../docs/samples/parallel-async-dag.yaml");

        then:
        JSONObject responseJson = sendApiRequest(url, contentType, requestData);

        expect:
        responseJson.statusCode == 200
        responseJson.getJSONObject("responseContent").getBoolean("ret") == true
    }

    def "parallel async sample submit dag task"() {
        when:
        String url = "http://localhost:8080/flow/submit.json?descriptor_id=rillFlowSample:parallelAsyncTask";
        String contentType = "application/json";
        String requestData = "{\"rand_num\":20}";

        then:
        JSONObject responseJson = sendApiRequest(url, contentType, requestData);

        expect:
        responseJson.statusCode == 200
        responseJson.getJSONObject("responseContent").getString("execution_id") != ""
    }

    def "ref sample add dag task"() {
        when:
        String url = "http://localhost:8080/flow/bg/manage/descriptor/add_descriptor.json?business_id=rillFlowSample&feature_name=subdagTask&alias=release";
        String contentType = "text/plain";
        String requestData = readFileContent("../docs/samples/ref-dag.yaml");

        then:
        JSONObject responseJson = sendApiRequest(url, contentType, requestData);

        expect:
        responseJson.statusCode == 200
        responseJson.getJSONObject("responseContent").getBoolean("ret") == true
    }

    def "ref sample submit dag task"() {
        when:
        String url = "http://localhost:8080/flow/submit.json?descriptor_id=rillFlowSample:subdagTask";
        String contentType = "application/json";
        String requestData = "{\"parent_rand_num\":20}";

        then:
        JSONObject responseJson = sendApiRequest(url, contentType, requestData);

        expect:
        responseJson.statusCode == 200
        responseJson.getJSONObject("responseContent").getString("execution_id") != ""
    }

    private String readFileContent(String filePath) {
        try {
            // 读取文件内容并返回
            return new File(filePath).text;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private JSONObject sendApiRequest(String url, String contentType, String requestData) {
        HttpClient httpClient = HttpClientBuilder.create().build();
        JSONObject jsonObject = new JSONObject();
        try {
            HttpPost httpPost = new HttpPost(url);
            httpPost.addHeader("Content-Type", contentType);

            httpPost.setEntity(new StringEntity(requestData, ContentType.create(contentType)));

            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();

            int statusCode = response.getStatusLine().getStatusCode();
            String responseContent = new JsonSlurper().parse(entity.content).toString()
            jsonObject.put("statusCode", statusCode);
            jsonObject.put("responseContent", responseContent);
            return jsonObject;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}