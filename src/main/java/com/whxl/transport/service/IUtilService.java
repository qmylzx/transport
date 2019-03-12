package com.whxl.transport.service;

import com.whxl.transport.exception.EtlException;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.List;

public interface IUtilService {

    String verifyUser(Cookie[] cookies, List<Object> intValues) throws EtlException, IOException;

    void submitTask(Runnable r);
}
