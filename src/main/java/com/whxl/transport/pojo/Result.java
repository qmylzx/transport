package com.whxl.transport.pojo;

import com.alibaba.fastjson.JSON;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
/*
code
40000  正常
40001  输入参数为空!
40002  未选择文件
40003  文件上传到web服务器失败!
40004  文件上传至HDFS失败!
40005  数据库导入至失败

40010  鉴权失败，权限不足
40011  code 未登录或登录信息过期
40012  result==null 请求无数据
40013  鉴权模块不可用

*/
public class Result {
    private String code;
    private Object data;

    private Result(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public static Result setCode(String code) {
        return new Result(code);
    }

    public Object getData() {
        return data;
    }

    public Result setData(Object data) {
        this.data = data;
        return this;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
