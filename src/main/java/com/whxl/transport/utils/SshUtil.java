package com.whxl.transport.utils;


import com.jcraft.jsch.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/*
 * 这里账号密码端口以及host后面可以用配置文件管理,spring注入即可。
 * */
public class SshUtil {
    private static volatile SshUtil sshUtil;
    private static String userName = "root";
    private static int port = 10122;
    private static String host = "115.156.128.241";
    private static String password = "xl";
    private Session session;
    private ChannelSftp channelSftp;
    private ChannelExec channelExec;
    private final static Log logger = LogFactory.getLog(SshUtil.class);

    private SshUtil() {
    }

    public void getConnection() throws JSchException {
        logger.debug("SshUtil getConnection");
        JSch jSch = new JSch(); //创建JSch对象
        session = jSch.getSession(userName, host, port);//根据用户名，主机ip和端口获取一个Session对象
        session.setPassword(password); //设置密码
        Properties config = new Properties();
        config.put("PreferredAuthentications", "password");
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);//为Session对象设置properties
        session.setTimeout(5000);//设置超时
        session.connect();//通过Session建立连接
        logger.debug("SshUtil connect open");
    }


    public void close() {
        logger.debug("SshUtil connect close");
        session.disconnect();
    }


    public void upLoad(String localDir, String romoteDir,String deviceType,long batchId)
                                                            throws JSchException, SftpException {
        logger.debug("SshUtil upLoad file , localDir = "
                +localDir+", romoteDir = "+romoteDir+" , deviceType:"+deviceType+",batchId:"+batchId);
        channelSftp = (ChannelSftp) session.openChannel("sftp");
        channelSftp.connect();
        logger.debug("channelSftp connect");
        channelSftp.cd(romoteDir);
        if(!isDirExist(deviceType)){
            logger.debug("mkdir"+deviceType);
            channelSftp.mkdir(deviceType);

        }
        channelSftp.cd(deviceType);
        String t = String.valueOf(batchId);
        if(!isDirExist(String.valueOf(batchId))){
            logger.debug("mkdir"+batchId);
            channelSftp.mkdir(t);
        }
        channelSftp.cd(t);
        channelSftp.put(localDir, "./");
        logger.debug("上传至linux成功");
        channelSftp.quit();
        logger.debug("upLoad 完成");
    }

    /**
     * 判断目录是否存在
     */
    private boolean isDirExist(String directory) {
        logger.debug("isDirExist start");
        boolean isDirExistFlag = false;
        try {
            SftpATTRS sftpATTRS = channelSftp.lstat(directory);
            isDirExistFlag = true;
            return sftpATTRS.isDir();
        } catch (Exception e) {
            if (e.getMessage().toLowerCase().equals("no such file")) {
                isDirExistFlag = false;
            }
        }
        logger.debug("isDirExist result : " +isDirExistFlag);
        return isDirExistFlag;
    }


    public void executeCommand(String command) throws JSchException, IOException {
        logger.debug("method executeCommand() :"+ command);
        channelExec = (ChannelExec) session.openChannel("exec");
        channelExec.setCommand(command);
        channelExec.setInputStream(null);
        channelExec.setErrStream(System.err);
        InputStream in = channelExec.getInputStream();
        channelExec.connect();
        logger.debug("connect success");
        byte[] tmp = new byte[1024];
        while (true) {
            while (in.available() > 0) {
                int i = in.read(tmp, 0, 1024);
                if (i < 0) break;
                System.out.print(new String(tmp, 0, i));
            }
            if (channelExec.isClosed()) {
                if (in.available() > 0) continue;
                logger.debug("exit-status: " + channelExec.getExitStatus());
                break;
            }
        }
        channelExec.disconnect();
    }

    public static SshUtil getSshUtil() {
        logger.debug("getSshUtil");
        if (null == sshUtil) {
            synchronized (SshUtil.class) {
                if (null == sshUtil) {//这里进行property配置读入
                    logger.debug("return new sshUtil bean");
                    sshUtil = new SshUtil();
                    return sshUtil;
                }
            }
        }
        logger.debug("Already exist return sshUtil bean now");
        return sshUtil;
    }
}
