package com.atguigu.gmallpublishe.servers.impl;

import com.atguigu.gmallpublishe.mapper.DauMapper;
import com.atguigu.gmallpublishe.servers.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: shade
 * @date: 2022/6/22 14:59
 * @description:
 */
@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    private DauMapper dauMapper;

    @Override
    public int getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHours(String date) {

        List<Map> maps = dauMapper.selectDauTotalHourMap(date);

        HashMap<String, Long> result = new HashMap();

        for (Map map : maps) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }

        return result;
    }
}
