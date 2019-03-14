package com.whxl.transport.serviceimpl;


import com.jcraft.jsch.JSchException;
import com.whxl.transport.dao.IBatchDao;
import com.whxl.transport.exception.EtlException;
import com.whxl.transport.pojo.BatchInfo;
import com.whxl.transport.pojo.Result;
import com.whxl.transport.service.IBatchService;
import com.whxl.transport.utils.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.util.List;

@Service
public class BatchServiceImpl implements IBatchService {
    @Autowired
    private IBatchDao iBatchDao;

    private final Log logger = LogFactory.getLog(getClass());

    @Override
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.DEFAULT)
    public long insertFile(String deviceType, String executeUser, String executeDate,
                           String comments) {
        logger.debug("insertFile()");
        BatchInfo batchInfo = new BatchInfo();
        batchInfo.setDeviceType(deviceType);
        batchInfo.setExecuteUser(executeUser);
        batchInfo.setExecuteDate(executeDate);
        batchInfo.setComments(comments);
        logger.debug("return insertFile");
        iBatchDao.insertFile(batchInfo);
        return batchInfo.getId();
    }

    @Override
    public List<BatchInfo> selectBatchInfos() {
        logger.debug("selectBatchInfos()");
        return iBatchDao.selectBatchInfos();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.DEFAULT)
    public String deleteBatch(Long batchId) {
        String deviceType = iBatchDao.getTypeById(batchId);
        int x = iBatchDao.deleteBatch(batchId);
        logger.debug("删除记录数量为:" + x+" , deviceType = "+deviceType);
        if (x != 0) {  //   有记录
            logger.debug("开始删除hdfs数据...");
            try {
                FileUtils.removeBatchFile(batchId,deviceType);
                logger.debug("删除成功!!!");
                return "40000";
            } catch (JSchException je) {
                logger.debug("ssh连接服务器失败!");
                return "40008";
            } catch (IOException ioe) {
                logger.debug("执行remote shell命令错误");
                return "40009";
            }
        }
        logger.debug("不存在记录!");
        return "40030";//   无记录
    }

}
