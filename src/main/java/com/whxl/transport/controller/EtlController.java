package com.whxl.transport.controller;


import com.jcraft.jsch.JSchException;
import com.whxl.transport.exception.EtlException;
import com.whxl.transport.pojo.BatchInfo;
import com.whxl.transport.pojo.Result;
import com.whxl.transport.service.IBatchService;
import com.whxl.transport.service.IUtilService;

import com.whxl.transport.utils.FileUtils;
import com.whxl.transport.utils.asynchronous.DatabaseLazyTask;
import com.whxl.transport.utils.asynchronous.FileLazyTask;

import com.whxl.transport.utils.asynchronous.MyKafkaConsumer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import org.springframework.util.StringUtils;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;


@Controller
@RequestMapping("/transport/v1")
public class EtlController {
    private final Log logger = LogFactory.getLog(getClass());
    @Autowired
    private IBatchService iBatchService;
    @Autowired
    private IUtilService iUtilService;

    private static LinkedHashMap<String, Runnable> task = new LinkedHashMap<String, Runnable>(1024, 1) {
        private final Object lock = new Object();

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Runnable> eldest) {
            return size() > 1023;
        }

        @Override
        public Runnable put(String key, Runnable value) {
            synchronized (lock) {
                return super.put(key, value);
            }
        }
    };

    private static ExecutorService pool = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
            new SynchronousQueue<>());

    private static ConcurrentHashMap<String, Runnable> streamTask = new ConcurrentHashMap<>();

    @RequestMapping(value = "/fileUpload", method = RequestMethod.POST)
    @ResponseBody
    public Result fileUpload(MultipartFile file, HttpServletRequest request) throws EtlException {
        logger.debug("start fileUpload");
        String comment = request.getParameter("comment");

        String deviceType = request.getParameter("deviceType");

        String mappings = request.getParameter("mappings"); //映射关系

        String deviceId = request.getParameter("deviceId");  // id  对应的列

        if (file == null) {
            logger.debug("file  is null");
            return Result.setCode("40002");
        }
        String filePath = request.getSession().getServletContext().getRealPath("/")
                + file.getOriginalFilename();
        if (!filePath.endsWith("csv")) {  //这里仅对文件尾缀做判断
            logger.debug("file  is not csv");
            return Result.setCode("40005");
        }
        logger.debug("filePath:" + filePath);


        if (StringUtils.isEmpty(mappings) || StringUtils.isEmpty(deviceType) ||
                StringUtils.isEmpty(comment) || StringUtils.isEmpty(deviceId)) {
            logger.debug("parameter is null");
            return Result.setCode("40001");
        }
        String userName;
        //auth
        try {
            List<Object> list = new ArrayList<>();
            list.add(1);
            logger.debug("权限码 ：" + list.toString());
            userName = iUtilService.verifyUser(request.getCookies(), list);
            logger.debug("userName ：" + userName);
        } catch (IOException e) {
            logger.debug("无权限！");
            return Result.setCode("40013");
        }
        try {
            logger.debug("上传文件到服务器!");
            file.transferTo(new File(filePath));
        } catch (Exception e) {
            logger.debug("上传文件到服务器失败!");
            return Result.setCode("40003");
        }
        //modify  field row  to device_id  and add distinct d_id to list   // NIO

        logger.debug("开始插入批次!");
        long batchId = iBatchService.insertFile(deviceType, userName,
                LocalDate.now().toString(), comment);
        logger.debug("批次号码为：" + batchId);

        Runnable iTask = new FileLazyTask(batchId, filePath, deviceType,
                file.getOriginalFilename(), mappings,deviceId);
        logger.debug("添加任务到线程池...");
        task.put(String.valueOf(batchId), iTask);
        iUtilService.submitTask(iTask);
        logger.debug("任务已经提交....");
        return Result.setCode("40000");
    }

    @RequestMapping(value = "/batch", method = RequestMethod.GET)
    @ResponseBody
    public Result batch(HttpServletRequest request) throws EtlException {
        logger.debug("进入 batch()");
        //auth
        try {
            List<Object> list = new ArrayList<>();
            list.add(1);
            // list.add(2);
            logger.debug("开始校验权限:" + list.toString());
            iUtilService.verifyUser(request.getCookies(), list);
        } catch (IOException e) {
            logger.debug("校验权限失败");
            return Result.setCode("40013");
        }
        logger.debug("开始查询！");
        List<BatchInfo> list = iBatchService.selectBatchInfos();
        logger.debug("查询完毕！");
        return Result.setCode("40000").setData(list);
    }

    @RequestMapping(value = "/db", method = RequestMethod.POST)
    @ResponseBody
    public Result db(HttpServletRequest request) throws EtlException {
        logger.debug("进入 db()");

        String deviceType = request.getParameter("deviceType");
        String comment = request.getParameter("comment");
        String mappings = request.getParameter("mappings");

        String dbName = request.getParameter("dbName");
        String tableName = request.getParameter("tableName");

        String ip = request.getParameter("ip");
        String port = request.getParameter("port");
        String username = request.getParameter("username");
        String pwd = request.getParameter("password");
        String deviceId = request.getParameter("deviceId");  // id  对应的Name

        if (StringUtils.isEmpty(deviceType) || StringUtils.isEmpty(comment) || StringUtils.isEmpty(mappings)
                || StringUtils.isEmpty(dbName) || StringUtils.isEmpty(tableName)
                || StringUtils.isEmpty(ip) || StringUtils.isEmpty(port)
                || StringUtils.isEmpty(username) || StringUtils.isEmpty(pwd)||StringUtils.isEmpty(deviceId)) {
            return Result.setCode("40001");
        }
        String userName;
        //auth
        try {
            List<Object> list = new ArrayList<>();
            list.add(1);
            list.add(2);
            logger.debug("开始校验权限:" + list.toString());
            userName = iUtilService.verifyUser(request.getCookies(), list);
        } catch (IOException e) {
            logger.debug("校验权限失败");
            return Result.setCode("40013");
        }

        long batchId = iBatchService.insertFile(deviceType,
                userName, LocalDate.now().toString(), comment);
        Runnable iTask = new DatabaseLazyTask(batchId, deviceType, mappings, dbName, tableName,
                username, pwd, ip, port,deviceId);
        logger.debug("添加任务到线程池...");
        task.put(String.valueOf(batchId), iTask); //添加任务
        iUtilService.submitTask(iTask);
        logger.debug("任务已经提交....");
        return Result.setCode("40000");
    }


    @RequestMapping(value = "/getstatus", method = RequestMethod.GET)
    @ResponseBody
    public Result getStatus(HttpServletRequest request) throws EtlException {
        //auth
        try {
            List<Object> list = new ArrayList<>();
            list.add(1);
            list.add(2);
            logger.debug(" 开始校验权限:" + list.toString());
            iUtilService.verifyUser(request.getCookies(), list);
        } catch (IOException e) {
            logger.debug("校验权限失败");
            return Result.setCode("40013");
        }
        Collection<Runnable> taskSet = task.values();
        if (taskSet.size() == 0) {
            return Result.setCode("40022");
        }
        return Result.setCode("40000").setData(taskSet);
    }

    @RequestMapping(value = "/stream", method = RequestMethod.POST)
    @ResponseBody
    public Result stream(HttpServletRequest request) throws EtlException {
        String ip = request.getParameter("ip");
        String port = request.getParameter("port");
        String topic = request.getParameter("topic");
        String mappings = request.getParameter("mappings");
        String deviceType = request.getParameter("deviceType");
        String deviceId = request.getParameter("deviceId");  // id  对应的列
        if (StringUtils.isEmpty(ip) || StringUtils.isEmpty(port) || StringUtils.isEmpty(topic)
                || StringUtils.isEmpty(mappings) || StringUtils.isEmpty(deviceType)|| StringUtils.isEmpty(deviceId)) {
            throw new EtlException("40001");
        }
        try {
            List<Object> list = new ArrayList<>();
            list.add(1);
            list.add(2);
            logger.debug("开始校验权限:" + list.toString());
            iUtilService.verifyUser(request.getCookies(), list);
        } catch (IOException e) {
            logger.debug("校验权限失败");
            return Result.setCode("40013");
        }
        Runnable iTask = new MyKafkaConsumer(ip, port, topic, mappings, deviceType,deviceId);
        streamTask.put(String.valueOf(ip + ":" + port + "," + topic), iTask); //添加任务
        pool.submit(iTask);
        return Result.setCode("40000");
    }


    @RequestMapping(value = "/getstreamstatus", method = RequestMethod.GET)
    @ResponseBody
    public Result getKafkaStatus(HttpServletRequest request) throws EtlException {
        try {
            List<Object> list = new ArrayList<>();
            list.add(1);
            list.add(2);
            logger.debug("开始校验权限 : " + list.toString());
            iUtilService.verifyUser(request.getCookies(), list);
        } catch (IOException e) {
            logger.debug("校验权限失败");
            return Result.setCode("40013");
        }
        Collection<Runnable> taskSet = streamTask.values();
        if (taskSet.size() == 0) {
            return Result.setCode("40022");
        }
        return Result.setCode("40000").setData(taskSet);
    }

    @RequestMapping(value = "/batch/{batchId}", method = RequestMethod.DELETE)
    @ResponseBody
    public Result removeBatch(HttpServletRequest request, @PathVariable("batchId") Long batchId) throws EtlException {
        try {
            List<Object> list = new ArrayList<>();
            list.add(1);
            list.add(2);
            logger.debug(" 开始校验权限 : " + list.toString());
            iUtilService.verifyUser(request.getCookies(), list);
        } catch (IOException e) {
            logger.debug("校验权限失败");
            return Result.setCode("40013");
        }
        return Result.setCode(iBatchService.deleteBatch(batchId));
    }

    @RequestMapping(value = "/stream/stop", method = RequestMethod.POST)
    @ResponseBody
    public Result stopBatch(HttpServletRequest request) throws EtlException {
        try {
            List<Object> list = new ArrayList<>();
            list.add(1);
            list.add(2);
            logger.debug(" 开始校验权限  : " + list.toString());
            iUtilService.verifyUser(request.getCookies(), list);
        } catch (IOException e) {
            logger.debug("校验权限失败");
            return Result.setCode("40013");
        }
        String ip = request.getParameter("ip");
        String port = request.getParameter("port");
        String topic = request.getParameter("topic");
        if (StringUtils.isEmpty(ip) || StringUtils.isEmpty(port) || StringUtils.isEmpty(topic)) {
            throw new EtlException("40001");
        }
        //stop
        MyKafkaConsumer task = ((MyKafkaConsumer) streamTask.get(ip + ":" + port + "," + topic));
        if (task == null) {
            return Result.setCode("40022");
        }
        task.stop();
        return Result.setCode("40000");
    }

}
