package com.atguigu.gmall0826.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.sql.SQLOutput;
import java.util.List;

public class CanalClient {

    public static void main(String[] args) {

        //1. 连接canal server
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111),
                "example", "", "");

        while (true) {

            canalConnector.connect();
            //2. 抓取数据
            canalConnector.subscribe("*.*");

            Message message = canalConnector.get(100);
            if(message.getEntries().size() == 0) {
                System.out.println("没有数据，休息一会");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {

                for (CanalEntry.Entry entry : message.getEntries()) {
                    if(CanalEntry.EntryType.ROWDATA == entry.getEntryType()) {
                        ByteString storeValue = entry.getStoreValue();

                        CanalEntry.RowChange rowChange = null;

                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }

                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        String tableName = entry.getHeader().getTableName();

                        CanalHandler canalHandler = new CanalHandler(eventType, tableName, rowDatasList);
                        canalHandler.handle();
                    }
                }
            }
        }

    }
}
