package com.whxl.transport.serviceimpl;


import com.whxl.transport.dao.IBatchDao;
import com.whxl.transport.pojo.BatchInfo;
import com.whxl.transport.service.IBatchService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class BatchServiceImpl implements IBatchService {
    @Autowired
    private IBatchDao iBatchDao;

    private final Log logger = LogFactory.getLog(getClass());

    @Override
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.DEFAULT)
    public long insertFile(String deviceType,String executeUser, String executeDate,
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
}
