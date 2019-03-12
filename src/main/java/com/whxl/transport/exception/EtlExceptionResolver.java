package com.whxl.transport.exception;

import com.alibaba.fastjson.JSON;

import com.whxl.transport.pojo.Result;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerExceptionResolver;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Configuration
public class EtlExceptionResolver implements HandlerExceptionResolver {
    @Override
    public ModelAndView resolveException(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, Object handler, Exception e) {
        e.printStackTrace();
        if(e instanceof EtlException){
            HandlerMethod handlerMethod = (HandlerMethod)handler;
            ResponseBody responseBody = handlerMethod.getMethod().getAnnotation(ResponseBody.class);
            if(responseBody!=null){
                String json = JSON.toJSONString(Result.setCode(((EtlException)e).getCode()));
                httpServletResponse.setCharacterEncoding("UTF-8");
                httpServletResponse.setContentType("application/json;charset=utf-8");
                try {
                    httpServletResponse.getWriter().write(json);
                    httpServletResponse.getWriter().flush();
                }catch (IOException e1){
                    e1.printStackTrace();
                }
            }
        }
        return new ModelAndView();
    }
}
