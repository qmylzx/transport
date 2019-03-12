package com.whxl.transport.utils.asynchronous;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import com.whxl.transport.exception.EtlException;
import com.whxl.transport.utils.FileUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;



public class FileLazyTask implements Runnable{

    private final Log logger = LogFactory.getLog(getClass());
    private long batchId;
    private String path;
    private String deviceType;
    private String filename;
    private String mappings;
    private String code = "40020";


    public FileLazyTask(long batchId, String path, String deviceType, String filename, String mappings) {
        this.batchId = batchId;
        this.path = path;
        this.deviceType = deviceType;
        this.filename = filename;
        this.mappings = mappings;

    }

    @Override
    public void run() {
        try {
            logger.debug("开始上传文件到hdfs");
            FileUtils.uploadFile(path, deviceType, batchId, filename, mappings);
            code = "40021";
            logger.debug("成功！");
        } catch (Exception e) {
            logger.debug("上传文件失败!");
            if (e instanceof JSchException) {
                logger.debug("ssh连接服务器失败!");
                code = "40008";
            } else if (e instanceof IOException) {
                logger.debug("执行remote shell命令错误");
                code = "40009";
            } else if (e instanceof SftpException) {
                logger.debug("文件上传至HDFS失败!");
                code = "40004";
            } else if (e instanceof EtlException) {
                logger.debug("自定义异常!");
                code = ((EtlException) e).getCode();
            }
        } finally {
            logger.debug("任务状态 : " + toString());
        }
    }

    public long getBatchId() {
        return batchId;
    }


    public String getPath() {
        return path;
    }


    public String getDeviceType() {
        return deviceType;
    }


    public String getFilename() {
        return filename;
    }


    public String getMappings() {
        return mappings;
    }


    public String getCode() {
        return code;
    }


    @Override
    public String toString() {
        return "batchId:" + getBatchId() + "\npath: " + getPath() + "\ndeviceType = "
                + getDeviceType() + "\nfilename = " + getFilename() + "\nmappings = " + getMappings() + "\ncode = " + getCode();
    }

}
