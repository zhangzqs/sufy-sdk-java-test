package com.sufy.sdktest;

import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HttpClientRecorder implements SdkHttpClient {
    private final SdkHttpClient httpClient;
    private List<HttpRecord> records;

    public static class HttpRecord {
        public HttpExecuteRequest request;
        public HttpExecuteResponse response;
    }

    public HttpClientRecorder(SdkHttpClient httpClient) {
        this.httpClient = httpClient;
        this.records = null;
    }

    public void startRecording() {
        this.records = new ArrayList<>();
    }

    public List<HttpRecord> stopAndGetRecords() {
        List<HttpRecord> result = Collections.unmodifiableList(this.records);
        records = null;
        return result;
    }


    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
        ExecutableHttpRequest ehr = httpClient.prepareRequest(request);

        return new ExecutableHttpRequest() {
            @Override
            public HttpExecuteResponse call() throws IOException {
                HttpExecuteResponse response = ehr.call();
                if (records != null) {
                    HttpRecord record = new HttpRecord();
                    record.request = request;
                    record.response = response;
                    records.add(record);
                }
                return response;
            }

            @Override
            public void abort() {
                ehr.abort();
            }
        };
    }


    @Override
    public String clientName() {
        return httpClient.clientName();
    }

    @Override
    public void close() {
        httpClient.close();
    }
}
