package com.whxl.transport.exception;
/*
code
40000  正常
40001  输入参数为空!
40002  未选择文件
40003  文件上传到web服务器失败!
40004  文件上传至HDFS失败!
40005  文件非CSV
40006  数据库导入至失败
40007  mappings json 格式错误
40008  ssh连接服务器失败
40009  执行remote shell命令错误

40010  鉴权失败，权限不足
40011  code 未登录或登录信息过期
40012  result==null 请求无数据
40013  鉴权模块不可用

40020  任务处理中
40021  任务完成
40022  任务为空
40023  加载配置失败或数据导入hdfs失败
40024  流数据导入停止

40030  记录不存在

*/
public class EtlException extends Exception{
    private String code;

    public EtlException(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }
}
