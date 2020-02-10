package com.atguigu.gmall0826.gmall0826publisher.service.impl;

import com.atguigu.gmall0826.gmall0826publisher.mapper.DauMapper;
import com.atguigu.gmall0826.gmall0826publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Override
    public Long getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHourCount(String date) {

        List<Map> maps = dauMapper.selectDauHourCount(date);

        Map resultMap = new HashMap();

        for (Map map : maps) {
            resultMap.put(map.get("LOGHOUR"), map.get("CT"));
        }

        return resultMap;
    }
}
