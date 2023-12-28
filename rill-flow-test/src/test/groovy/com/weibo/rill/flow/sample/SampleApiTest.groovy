package com.weibo.rill.flow.sample

import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Paths

import static org.junit.jupiter.api.Assertions.assertEquals

class SampleApiTest extends Specification {
    class ApiResponse {
        int statusCode;
        String responseContent;

        ApiResponse(int statusCode, String responseContent) {
            this.statusCode = statusCode;
            this.responseContent = responseContent;
        }

        int getStatusCode() {
            return statusCode
        }

        void setStatusCode(int statusCode) {
            this.statusCode = statusCode
        }

        String getResponseContent() {
            return responseContent
        }

        void setResponseContent(String responseContent) {
            this.responseContent = responseContent
        }
    }


    @Test
    @Order(1)
    public void testChoiceSampleAddDescriptor() {
        String url = "http://localhost:8080/flow/bg/manage/descriptor/add_descriptor.json?business_id=rillFlowSample&feature_name=choiceSample&alias=release";
        String contentType = "text/plain";
        String requestData = readFileContent("../docs/samples/choice-sample.yaml");
        // 执行 API 请求并获取响应
        ApiResponse response = sendApiRequest(url, contentType, requestData);
        // 使用测试断言来验证测试结果
        assertEquals(200, response.getStatusCode());
    }

    @Test
    @Order(2)
    public void testChoiceSampleSubmit() {
        String url = "http://localhost:8080/flow/submit.json?descriptor_id=rillFlowSample:choiceSample";
        String contentType = "application/json";
        String requestData = "{\"input_num\":10}";
        ApiResponse response = sendApiRequest(url, contentType, requestData);
        assertEquals(200, response.getStatusCode());
    }

    @Test
    @Order(3)
    public void testCallApiSampleAddDescriptor() {
        String url = "http://localhost:8080/flow/bg/manage/descriptor/add_descriptor.json?business_id=rillFlowSample&feature_name=callApiSample&alias=release";
        String contentType = "text/plain";
        String requestData = readFileContent("../docs/samples/call-api-sample.yaml");
        ApiResponse response = sendApiRequest(url, contentType, requestData);
        assertEquals(200, response.getStatusCode());
    }

    @Test
    @Order(4)
    public void testCallApiSampleSubmit() {
        String url = "http://localhost:8080/flow/submit.json?descriptor_id=rillFlowSample:callApiSample";
        String contentType = "application/json";
        String requestData = "{\"input_num\":10}";
        ApiResponse response = sendApiRequest(url, contentType, requestData);
        assertEquals(200, response.getStatusCode());
    }

    @Test
    @Order(5)
    public void testParallelAsyncTaskAddDescriptor() {
        String url = "http://localhost:8080/flow/bg/manage/descriptor/add_descriptor.json?business_id=rillFlowSample&feature_name=parallelAsyncTask&alias=release";
        String contentType = "text/plain";
        String requestData = readFileContent("../docs/samples/parallel-async-dag.yaml");
        ApiResponse response = sendApiRequest(url, contentType, requestData);
        assertEquals(200, response.getStatusCode());
    }

    @Test
    @Order(6)
    public void testParallelAsyncTaskSubmit() {
        String url = "http://localhost:8080/flow/submit.json?descriptor_id=rillFlowSample:parallelAsyncTask";
        String contentType = "application/json";
        String requestData = "{\"rand_num\":20}";
        ApiResponse response = sendApiRequest(url, contentType, requestData);
        assertEquals(200, response.getStatusCode());
    }

    @Test
    @Order(7)
    public void testSubDagTaskAddDescriptor() {
        String url = "http://localhost:8080/flow/bg/manage/descriptor/add_descriptor.json?business_id=rillFlowSample&feature_name=subdagTask&alias=release";
        String contentType = "text/plain";
        String requestData = readFileContent("../docs/samples/ref-dag.yaml");
        ApiResponse response = sendApiRequest(url, contentType, requestData);
        assertEquals(200, response.getStatusCode());
    }

    @Test
    @Order(8)
    public void testSubDagTaskSubmit() {
        String url = "http://localhost:8080/flow/submit.json?descriptor_id=rillFlowSample:subdagTask";
        String contentType = "application/json";
        String requestData = "{\"parent_rand_num\":20}";
        ApiResponse response = sendApiRequest(url, contentType, requestData);
        assertEquals(200, response.getStatusCode());
    }




    private String readFileContent(String filePath) {
        try {
            // 读取文件内容并返回
            return new String(Files.readAllBytes(Paths.get(filePath)));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private ApiResponse sendApiRequest(String url, String contentType, String requestData) {
        HttpClient httpClient = HttpClientBuilder.create().build();
        try {
            HttpPost httpPost = new HttpPost(url);
            httpPost.addHeader("Content-Type", contentType);

            // 设置请求体
            httpPost.setEntity(new StringEntity(requestData, ContentType.create(contentType)));

            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();

            // 获取响应状态码和内容
            int statusCode = response.getStatusLine().getStatusCode();
            String responseContent = EntityUtils.toString(entity);

            return new ApiResponse(statusCode, responseContent);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}