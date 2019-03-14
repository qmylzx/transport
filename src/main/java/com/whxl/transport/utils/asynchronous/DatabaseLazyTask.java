package com.whxl.transport.utils.asynchronous;

import com.jcraft.jsch.JSchException;
import com.whxl.transport.exception.EtlException;
import com.whxl.transport.utils.FileUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;


public class DatabaseLazyTask implements Runnable {
    private final Log logger = LogFactory.getLog(getClass());
    private long batchId;
    private String deviceType;
    private String mappings;
    private String dbName;
    private String tableName;
    private String username;
    private String pwd;
    private String ip;
    private String port;
    private String code = "40020";
    private String deviceId;

    public DatabaseLazyTask(long batchId, String deviceType, String mappings, String dbName, String tableName,
                            String username, String pwd, String ip, String port, String deviceId) {
        this.batchId = batchId;
        this.deviceType = deviceType;
        this.mappings = mappings;
        this.dbName = dbName;
        this.tableName = tableName;
        this.username = username;
        this.pwd = pwd;
        this.ip = ip;
        this.port = port;
        this.deviceId = deviceId;
    }


    @Override
    public void run() {
        try {
            logger.debug("开始上传文件到hdfs");
            FileUtils.dataBaseUpload(batchId, deviceType,
                    mappings, dbName, tableName, username, pwd, ip, port,deviceId);
            code = "40021"; //成功执行
            logger.debug("成功！");
        } catch (Exception e) {//异常
            logger.debug("上传database data到hive失败!");
            if (e instanceof JSchException) {
                logger.debug("ssh连接服务器失败");
                code = "40008";
            }
            if (e instanceof IOException) {
                logger.debug("40009  执行remote shell命令错误");
                code = "40009";
            }
            if (e instanceof EtlException) {
                logger.debug("自定义异常!");
                code = ((EtlException) e).getCode();
            }
        } finally {
            logger.debug("任务状态 : \n" + toString());
        }
    }

    public long getBatchId() {
        return batchId;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public String getMappings() {
        return mappings;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getUsername() {
        return username;
    }

    public String getPwd() {
        return pwd;
    }

    public String getIp() {
        return ip;
    }

    public String getPort() {
        return port;
    }

    public String getCode() {
        return code;
    }

    public String getDeviceId() {
        return deviceId;
    }

    @Override
    public String toString() {
        return "batchId:" + getBatchId() + "\ndeviceType = "
                + getDeviceType() + "\nmappings = " + getMappings()
                + "\ndbName = " + getDbName()
                + "\ntableName = " + getTableName()
                + "\nusername = " + getUsername()
                + "\npwd = " + getPwd()
                + "\nip = " + getIp()
                + "\nport = " + getPort()
                + "\ncode = " + getCode()
                + "\ndeviceId = " + getDeviceId();
    }

}
