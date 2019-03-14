package com.whxl.transport.dao;

import com.whxl.transport.pojo.BatchInfo;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface IBatchDao {

    Long insertFile(BatchInfo batchInfo);

    List<BatchInfo> selectBatchInfos();

    int deleteBatch(@Param("batchId") Long batchId);  //

    String getTypeById(@Param("batchId") Long batchId);
}