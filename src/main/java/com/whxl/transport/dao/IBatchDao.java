package com.whxl.transport.dao;

import com.whxl.transport.pojo.BatchInfo;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface IBatchDao {

    Long insertFile(BatchInfo batchInfo);

    List<BatchInfo> selectBatchInfos();
}