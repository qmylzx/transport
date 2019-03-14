package com.whxl.transport.utils;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import com.whxl.transport.exception.EtlException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class FileUtils {
    private final static Log logger = LogFactory.getLog(FileUtils.class);

    public static void dataBaseUpload(long batchId, String deviceType, String mappings, String dbName,
                                      String tableName, String username, String pwd, String ip, String port,String deviceId)
            throws JSchException, IOException, EtlException {
        //{ deviceid1 : "column1", deviceid2 : "column2" }
        logger.debug("dataBaseUpload start");
        SshUtil sshUtil = SshUtil.getSshUtil();
        sshUtil.getConnection();
        logger.debug("解析Mappings ....");
        Set<Map.Entry<String, Object>> keys ;
        try {
            keys = JSON.parseObject(mappings).entrySet();
            if(keys.size()==0){
                throw new EtlException("40007");
            }
        } catch (Exception e) {
            throw new EtlException("40007");
        }
        logger.debug("Mappings : " + keys.toString());

        sshUtil.executeCommand(analysisMapping(deviceType, batchId, keys, dbName, tableName, username,
                pwd, ip, port,deviceId));

        //建表
        StringBuilder sb = new StringBuilder();
        sb.append("source /etc/profile\n hive -e \"CREATE DATABASE IF NOT EXISTS etl;USE etl;");
        sb.append("CREATE external TABLE IF NOT EXISTS t"); //
        sb.append(deviceType);
        sb.append("_2");
        sb.append("(");
        int i = 1;

        for (Map.Entry<String, Object> entry : keys) {
            sb.append("v");
            sb.append(entry.getKey());
            sb.append(" string");
            if (i < keys.size()) {
                sb.append(",");
                i++;
            }
        }
        sb.append(", deviceid  string");
        sb.append(")PARTITIONED BY( batchid string) row format delimited fields terminated by '\t'");
        sb.append("location '/result1/");
        sb.append(deviceType);
        sb.append("';");
        sb.append("load data  inpath '/EtlDatabaseTmp/");
        sb.append(deviceType);
        sb.append("/");
        sb.append(batchId);
        sb.append("' into table t");
        sb.append(deviceType);
        sb.append("_2");
        sb.append(" PARTITION(batchid=");
        sb.append(batchId);
        sb.append(");\"");
        sshUtil.executeCommand(sb.toString());

        sshUtil.close();
        logger.debug("load to database success!");
    }

    //
    public static void uploadFile(String path, String deviceType, long batchId,
                                  String filename, String mappings,String deviceId) throws JSchException,
            IOException, SftpException, EtlException {
        logger.debug("FileUtils uploadFile");
        SshUtil sshUtil = SshUtil.getSshUtil();
        sshUtil.getConnection();
        sshUtil.upLoad(path, "/home", deviceType, batchId);  //   /home/deviceType/batchId
        logger.debug("FileUtils uploadFile to linux success ");
        sshUtil.executeCommand(analysisMapping(mappings, deviceType, batchId, filename,deviceId));
        sshUtil.close();
        logger.debug("uploadFile success!");
    }

    public static void stream(String mappings, String deviceType,String deviceId) throws EtlException, JSchException, IOException {
        StringBuilder sb;
        try {
            logger.debug("file 开始建hive表...\n开始解析mappings ：" + mappings + "\ndeviceType : "
                    + deviceType + " , deviceId : " + deviceId);
            JSONObject jsonObject = JSON.parseObject(mappings);
            Set<String> keys = jsonObject.keySet();
            if(keys.size()==0){
                throw new EtlException("40007");
            }
            String[] res = new String[keys.size()+1];
            keys.forEach(k -> {
                        res[Integer.parseInt(jsonObject.get(k).toString()) - 1] = k;
                    }
            );
            res[Integer.parseInt(deviceId)-1] = "deviceId";


            logger.debug("解析mappings eg:{ deviceId1: row, deviceId2: row}");
            sb = new StringBuilder();
            sb.append("source /etc/profile\nhive -e \"CREATE DATABASE IF NOT EXISTS etl;\nUSE etl;\n");
            sb.append("CREATE external TABLE IF NOT EXISTS t");
            sb.append(deviceType);
            sb.append("_3");
            sb.append("(\n");
            for (int i = 0, j = 1; i < res.length; i++) {
                if("deviceId".equals(res[i])){
                    sb.append(" deviceId");
                    sb.append(" string");
                }else {
                    sb.append(" v");
                    sb.append(res[i]);
                    sb.append(" string");
                }
                if (j < res.length) {
                    j++;
                    sb.append(",\n");
                }
            }
            sb.append(")row format delimited fields terminated by ','\n");
            sb.append("location '/result2/");
            sb.append(deviceType);
            sb.append("';\"\nhadoop fs -mkdir  /result2\nhadoop fs -mkdir  /result2/");
            sb.append(deviceType);
            sb.append("\nhadoop fs -touchz  /result2/");
            sb.append(deviceType);
            sb.append("/");
            sb.append("data.csv");
        } catch (Exception e) {
            throw new EtlException("40007");
        }
        SshUtil sshUtil = SshUtil.getSshUtil();
        sshUtil.getConnection();
        sshUtil.executeCommand(sb.toString());
        sshUtil.close();
    }

    public static void removeBatchFile(Long batchId, String deviceType)throws JSchException, IOException {
        //  hdfs api  delete   批次唯一  搜索 result  result1  result2
        logger.debug("start removeBatchFile batchid = "+batchId+",deviceType = "+deviceType);
        SshUtil sshUtil = SshUtil.getSshUtil();
        sshUtil.getConnection();
        StringBuilder sb = new StringBuilder();
        sb.append("source /etc/profile\nhadoop fs -rm -r /result");
        sb.append("/");
        sb.append(deviceType);
        sb.append("/batchid=");
        sb.append(batchId);
        sb.append("\nhadoop fs -rm -r /result1");
        sb.append("/");
        sb.append(deviceType);
        sb.append("/batchid=");
        sb.append(batchId);
        sshUtil.executeCommand(sb.toString());
        sshUtil.close();
        logger.debug("RemoveBatchFile success!");
    }


    //file import
    private static String analysisMapping(String mappings, String deviceType, long batchId, String filename,String deviceId) throws EtlException {
        StringBuilder sb;
        try {
            logger.debug("file 开始建hive表...\n开始解析mappings ：" + mappings + "\ndeviceType : "
                    + deviceType + "\nbatchId: " + batchId + "\nfilename : " + filename + " , deviceId : " + deviceId);
            JSONObject jsonObject = JSON.parseObject(mappings);

            Set<String> keys = jsonObject.keySet();
            if(keys.size()==0){
                throw new EtlException("40007");
            }
            String[] res = new String[keys.size()+1];
            keys.forEach(k -> {
                        res[Integer.parseInt(jsonObject.get(k).toString()) - 1] = k;
                    }
            );
            res[Integer.parseInt(deviceId)-1] = "deviceId";

            logger.debug("解析mappings eg:{ deviceId1: row, deviceId2: row}");
            sb = new StringBuilder();
            sb.append("source /etc/profile\nhive -e \"CREATE DATABASE IF NOT EXISTS etl;\nUSE etl;\n");
            sb.append("CREATE external TABLE IF NOT EXISTS t");
            sb.append(deviceType);
            sb.append("_1");
            sb.append("(\n");
            for (int i = 0, j = 1; i < res.length; i++) {
                if("deviceId".equals(res[i])){
                    sb.append(" deviceid");
                    sb.append(" string");
                }else {
                    sb.append(" v");
                    sb.append(res[i]);
                    sb.append(" string");
                }
                if (j < res.length) {
                    j++;
                    sb.append(",\n");
                }
            }
            sb.append(")PARTITIONED BY( batchid string)\nrow format delimited fields terminated by ','\n");
            sb.append("location '/result/");
            sb.append(deviceType);
            sb.append("';\n");
            sb.append("load data local inpath '/home/");
            sb.append(deviceType);
            sb.append("/");
            sb.append(batchId);
            sb.append("/");
            sb.append(filename);
            sb.append("' into table t");
            sb.append(deviceType);
            sb.append("_1");
            sb.append("\nPARTITION(batchid=");
            sb.append(batchId);
            sb.append(");\"");
        } catch (Exception e) {
            throw new EtlException("40007");
        }
        return sb.toString();
    }

    // db import
    private static String analysisMapping(String deviceType, long batchId,
                                          Set<Map.Entry<String, Object>> keys, String dbName, String tableName
            , String username, String pwd, String ip, String port,String deviceId) {
        logger.debug("db analysisMapping : " + " , deviceType:" +
                deviceType + " , batchId : " + batchId + " , dbName:" + dbName + " , tableName:" + tableName
                + " , username:" + username + " , pwd : " + pwd + " , ip : " + ip + " , port : " + port + " , deviceId : " + deviceId);
        StringBuilder sb = new StringBuilder();
        sb.append("source /etc/profile\nsqoop import ");
        sb.append("--connect jdbc:mysql://");
        sb.append(ip);
        sb.append(":");
        sb.append(port);
        sb.append("/");
        sb.append(dbName);
        sb.append("?characterEncoding=utf-8 ");
        sb.append("--username  ");
        sb.append(username);
        sb.append(" --password ");
        sb.append(pwd);
        sb.append(" --query 'select ");
        int i = 1;
        for (Map.Entry<String, Object> entry : keys) {
            sb.append(entry.getValue());
            sb.append(" as ");
            sb.append("v");
            sb.append(entry.getKey());
            if (i < keys.size()) {
                sb.append(",");
                i++;
            }
        }
        sb.append(",");
        sb.append(deviceId);
        sb.append(" as deviceid");

        sb.append(" from ");
        sb.append(tableName);
        sb.append(" where $CONDITIONS' ");
        sb.append("--target-dir ");  //HDFS path
        sb.append("/EtlDatabaseTmp/");
        sb.append(deviceType);
        sb.append("/");
        sb.append(batchId);
        sb.append(" ");
        sb.append("--delete-target-dir ");
        sb.append("--num-mappers 1 ");
        sb.append(" --direct ");
        sb.append("--fields-terminated-by '\t'");
        return sb.toString();
    }


}
/*
    drop table if exists default.usertest ;
    create table default.usertest(
　       id int,
　       name string,
    )ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;

load data inpath '/user/root/'+batchId+" "
into table default.usertest ;

select * from usertest;

sqoop import --connect jdbc:mysql://192.168.8.97:3306/db1?charset-utf8
--username root --password 123456
--table pd_info
--columns "pid,cid"
--check-column pid --incremental append --last-value 165
--hive-import
--hive-table pid_cids


        //存入hdfs
        //     linux /home/deviceType/batchId    ->     hdfs  /tmp/deviceType
        logger.debug("开始存入hdfs");
        sshUtil.executeCommand("/software/hadoop-2.7.6/bin/hadoop fs -put /home/" +
                deviceType + "/" + batchId + "  /tabletemp/" + deviceType);

CREATE DATABASE IF NOT EXISTS etl ;
USE etl;
CREATE external  TABLE IF NOT EXISTS deviceType(
   v1  string,
   v2  string,
   v3  string,
   v4  string,
   v5  string,
   v6  string
)
PARTITIONED BY (
  `batchid` string)
row format delimited
fields terminated by ','
location '/result/deviceType'
;

load data local inpath '/home/deviceType/21/user_info.csv' overwrite into table deviceType PARTITION(batchid=21);

SELECT * FROM deviceType WHERE batchid>= '21'  and v1 is not null;;
*/