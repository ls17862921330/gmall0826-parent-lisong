package com.atguigu.gmall0826.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall0826.canal.util.MyKafkaSender;
import com.atguigu.gmall0826.common.constant.GmallConstant;

import java.util.List;

public class CanalHandler {

    CanalEntry.EventType eventType;

    String tableName;

    List<CanalEntry.RowData> rowDataList;

    public CanalHandler(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDataList){
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDataList = rowDataList;
    }

    public void handle() {
        if(this.rowDataList != null && this.rowDataList.size() > 0) {
            if(this.tableName.equals("order_info") && this.eventType == CanalEntry.EventType.INSERT) {
                send(GmallConstant.KAFKA_TOPIC_ORDER);
            }else if(this.tableName.equals("order_detail") && this.eventType == CanalEntry.EventType.INSERT) {
                send(GmallConstant.KAFKA_TOPIC_ORDER_DETAIL);
            }else if(this.tableName.equals("user_info") && (this.eventType == CanalEntry.EventType.INSERT) || this.eventType == CanalEntry.EventType.UPDATE) {
                send(GmallConstant.KAFKA_TOPIC_USER);
            }
        }
    }

    public void send(String topic) {

        for (CanalEntry.RowData rowData : this.rowDataList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

            JSONObject jo = new JSONObject();

            for (CanalEntry.Column column : afterColumnsList) {
                System.out.println(column.getName() + "::" + column.getValue());
                jo.put(column.getName(), column.getValue());
            }
            /*
            //设置延迟，错开批次发往kafka
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/
            String log = jo.toJSONString();
            MyKafkaSender.send(topic,log);
        }
    }
}
