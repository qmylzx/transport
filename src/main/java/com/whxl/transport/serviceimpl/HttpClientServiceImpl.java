package com.whxl.transport.serviceimpl;

import com.whxl.transport.service.IHttpClientService;

import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import org.apache.commons.httpclient.Cookie;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.httpclient.HttpClient;


@Service
public class HttpClientServiceImpl implements IHttpClientService {

    @Autowired
    HttpClient httpClient;
    private final Log logger = LogFactory.getLog(getClass());

    /*
    Get params可以为空，                    所有新建请求应该携带cookie 保证鉴权
    Post、Put、Delete params为空则返回null
    */
    @Override
    public String Get(String url, Map<String, Object> params, javax.servlet.http.Cookie[] cookies) throws IOException {
        logger.debug("HttpClient Get:" + url);
        httpClient.getParams().setCookiePolicy(CookiePolicy.BROWSER_COMPATIBILITY);
        GetMethod getMethod = new GetMethod(url);

        getMethod.setRequestHeader("Cookie", transfer(cookies));

        if (params != null && params.size() != 0) {
            logger.debug("have params:" + params.toString());
            Set<String> keys = params.keySet();
            NameValuePair[] nameValuePair = new NameValuePair[params.size()];
            int i = 0;
            for (String key : keys) {
                nameValuePair[i] = new NameValuePair(key, params.get(key).toString());
                ++i;
            }
            getMethod.setQueryString(nameValuePair);
        }
        getMethod.addRequestHeader("Content-Type", "application/json; charset=UTF-8");

        // 打印出返回数据，检验一下是否成功
        int statusCode = httpClient.executeMethod(getMethod);
        logger.debug("Get statusCode =" + statusCode);
        return getMethod.getResponseBodyAsString().trim();
    }

    @Override
    public String Post(String url, Map<String, Object> params, javax.servlet.http.Cookie[] cookies) throws IOException {
        logger.debug("HttpClient Post:" + url);
        if (params == null || params.size() == 0) {
            logger.debug("params is null or size = 0");
            return null;
        }

        PostMethod postMethod = new PostMethod(url);

        postMethod.addRequestHeader("Content-Type", "application/json");
        postMethod.getParams().setParameter(HttpMethodParams.HTTP_CONTENT_CHARSET, "utf-8");//

        postMethod.setRequestHeader("Cookie", transfer(cookies));


        NameValuePair[] data = new NameValuePair[params.size()];

        int i = 0;
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            data[i] = new NameValuePair(entry.getKey(), entry.getValue().toString());
            i++;
        }
        postMethod.setRequestBody(data);

        int statusCode = httpClient.executeMethod(postMethod);
        logger.debug("Post statusCode =" + statusCode);
        return postMethod.getResponseBodyAsString().trim();
    }

    @Override
    public String Put(String url, Map<String, Object> params, javax.servlet.http.Cookie[] cookies) throws IOException {
        logger.debug("HttpClient Put:" + url);
        if (params == null || params.size() == 0) {
            logger.debug("params is null or size = 0");
            return null;
        }

        PutMethod putMethod = new PutMethod(url);

        putMethod.addRequestHeader("Content-Type", "application/json");

        putMethod.setRequestHeader("Cookie", transfer(cookies));

        putMethod.getParams().setParameter(HttpMethodParams.HTTP_CONTENT_CHARSET, "utf-8");

        List<NameValuePair> data = new ArrayList<NameValuePair>();

        for (String key : params.keySet()) {
            NameValuePair nameValuePair = new NameValuePair(key, params.get(key).toString());
            data.add(nameValuePair);
        }

        putMethod.setQueryString(data.toArray(new NameValuePair[0]));


        int statusCode = httpClient.executeMethod(putMethod);
        logger.debug("Post statusCode =" + statusCode);
        return putMethod.getResponseBodyAsString().trim();
    }

    @Override
    public String Delete(String url, Map<String, Object> params, javax.servlet.http.Cookie[] cookies) throws IOException {
        logger.debug("HttpClient Delete:" + url);
        if (params == null || params.size() == 0) {
            logger.debug("params is null or size = 0");
            return null;
        }

        DeleteMethod deleteMethod = new DeleteMethod(url);

        deleteMethod.addRequestHeader("Content-Type", "application/json");
        deleteMethod.getParams().setParameter(HttpMethodParams.HTTP_CONTENT_CHARSET, "utf-8");

        deleteMethod.setRequestHeader("Cookie", transfer(cookies));


        List<NameValuePair> data = new ArrayList<NameValuePair>();

        Set<String> keys = params.keySet();
        for (String key : keys) {
            NameValuePair nameValuePair = new NameValuePair(key, params.get(key).toString());
            data.add(nameValuePair);
        }

        deleteMethod.setQueryString(data.toArray(new NameValuePair[0]));


        int statusCode = httpClient.executeMethod(deleteMethod);
        logger.debug("Post statusCode =" + statusCode);
        return deleteMethod.getResponseBodyAsString();

    }

    private String transfer(javax.servlet.http.Cookie[] cookies) {
        logger.debug("transfer()");
        if (cookies == null || cookies.length == 0) {
            logger.debug("cookies is null or size = 0");
            return "";
        }
        StringBuffer temp = new StringBuffer();
        for (javax.servlet.http.Cookie cookie : cookies) {
            temp.append(new Cookie(null, cookie.getName(), cookie.getValue()).toString() + ";");
        }
        logger.debug("return transfer success");
        return temp.toString();
    }

}
