package com.atguigu.gmallpublishe.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublishe.servers.PublisherService;
import org.joda.time.LocalDate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: shade
 * @date: 2022/6/22 11:34
 * @description:
 */
@RestController
public class Controller {
    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String realtimeTotal(@RequestParam("date") String date){

        int total = publisherService.getDauTotal(date);

        ArrayList<Map> list = new ArrayList<Map>();

        HashMap<String, Object> duaMap = new HashMap<>();

        HashMap<String, Object> devMap = new HashMap<>();

        duaMap.put("id", "dau");
        duaMap.put("name","新增日活");
        duaMap.put("value",total);

        devMap.put("id", "new_mid");
        devMap.put("name", "新增设备");
        devMap.put("value", 233);

        list.add(duaMap);
        list.add(devMap);

        return JSONObject.toJSONString(list);

    }

    @RequestMapping("realtime-hours")
    public String realtimeHours(@RequestParam("id")String id,@RequestParam("date")String date){
        Map todaymap = publisherService.getDauTotalHours(date);

        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        Map yestermap = publisherService.getDauTotalHours(yesterday);

        HashMap<String, Map> result = new HashMap<>();

        result.put("yesterday", yestermap);
        result.put("today", todaymap);

        return JSONObject.toJSONString(result);

    }
}
