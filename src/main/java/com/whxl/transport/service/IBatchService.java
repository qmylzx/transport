package com.whxl.transport.service;

import com.whxl.transport.pojo.BatchInfo;

import java.util.List;

public interface IBatchService {
    /*
     * 插入新的批次的文件同时返回当前插入的批次
     * 使用重复读级别事?,保证插入后查询的id的正确
     * */
    long insertFile(String deviceType, String executeUser, String executeDate,
                    String comments);

    List<BatchInfo> selectBatchInfos();
}
