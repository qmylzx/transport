package com.whxl.transport.serviceimpl;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.whxl.transport.exception.EtlException;
import com.whxl.transport.service.IHttpClientService;
import com.whxl.transport.service.IUtilService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


import javax.servlet.http.Cookie;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

@Service
public class UtilServiceImpl implements IUtilService {
    @Autowired
    private IHttpClientService iHttpClientService;

    @Autowired
    private ExecutorService executorService;

    private final Log logger = LogFactory.getLog(getClass());
    /*
    List 为包含权限码 ，若不包含则抛出异常  ，这里 也可返回 空 自己选择  line 52

            //auth
            try {
                List<Object> list = new ArrayList<>();
                list.add(1);
                userName = iUtilService.verifyUser(request.getCookies(),list);
            } catch (IOException e) {
                Result result = Result.error();
                result.setMessage("error to get auth..");
                return result;
            }

    */
    public String verifyUser(Cookie[] cookies,List<Object> intValues) throws EtlException, IOException {
        logger.debug("start verifyUser");
        String userName;
        //verify auth
        String result = iHttpClientService.Get("http://auth:8080/usermanagement/v1/currentuser",
                null, cookies);
        if (result == null) {
            logger.debug("result is null");
            throw new EtlException("40012");
        }
        logger.debug("result is :"+result);
        JSONObject jsonObject = JSON.parseObject(result);
        logger.debug("code:"+jsonObject.get("code"));
        if(!jsonObject.get("code").equals(0)){
            logger.debug("code 不为 0 :"+!jsonObject.get("code").equals(0));
            throw new EtlException("40011");
        }

        logger.debug("current user json info:"+jsonObject.toJSONString());

        String tmp = jsonObject.get("user").toString();

        logger.debug("current code:"+jsonObject.get("code"));


        List<Object> privilege = JSON.parseArray(JSON.parseObject(tmp).get("privilege").toString());
        userName = JSON.parseObject(tmp).get("nickname").toString();
        System.out.println();
        if (!privilege.containsAll(intValues)) {
            logger.debug("权限不足! 当前为:"+privilege.toString()+"需要权限为:"+intValues.toString());
            throw new EtlException("40010");
        }
        logger.debug("verifyUser success userName"+userName);
        return userName;
    }

    public void submitTask(Runnable r) {
        logger.debug("submitTask");
        executorService.execute(r);
        logger.debug("submitTask success");
    }

}
