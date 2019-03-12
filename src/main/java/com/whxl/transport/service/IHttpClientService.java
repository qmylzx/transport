package com.whxl.transport.service;

import java.io.IOException;
import java.util.Map;

public interface IHttpClientService {

    String Get(String url, Map<String, Object> datas, javax.servlet.http.Cookie[] cookies) throws IOException;

    String Post(String url, Map<String, Object> datas,  javax.servlet.http.Cookie[] cookies) throws IOException;

    String Put(String url, Map<String, Object> datas,  javax.servlet.http.Cookie[] cookies) throws IOException;

    String Delete(String url, Map<String, Object> datas,  javax.servlet.http.Cookie[] cookies) throws IOException;
}
