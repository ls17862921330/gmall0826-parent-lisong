package com.atguigu.gmall0826.gmall0826publisher.service.impl;

import com.atguigu.gmall0826.gmall0826publisher.mapper.DauMapper;
import com.atguigu.gmall0826.gmall0826publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
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
    public Map getDauHourCount(String date) {  //调整变换结构
        //   maplist ===> [{"loghour":"12","ct",500},{"loghour":"11","ct":400}......]

        Map resultMap = new HashMap();

        List<Map> mapList = dauMapper.selectDauHourCount(date);
        // hourCountMap ==> {"12":500,"11":400,......}
        Map todayMap=new HashMap();
        for (Map map : mapList) {
            todayMap.put ( map.get("LOGHOUR"),map.get("CT"));
        }

        Map yesterdayMap = getYesterdayDauHourCount(date);

        resultMap.put("yesterday",yesterdayMap);
        resultMap.put("today",todayMap);

        return resultMap;
    }

    private Map getYesterdayDauHourCount(String date) {

        Map hourCountMap=new HashMap();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date yes = DateUtils.addDays(sdf.parse(date), -1);
            List<Map> mapList = dauMapper.selectDauHourCount(sdf.format(yes));
            for (Map map : mapList) {
                hourCountMap.put ( map.get("LOGHOUR"),map.get("CT"));
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return hourCountMap;
    }
}
