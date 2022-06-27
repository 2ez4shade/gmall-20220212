package com.shade.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.shade.utlis.MyKafkaUtils;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

/**
 * @author: shade
 * @date: 2022/6/24 11:09
 * @description:
 */
public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //创建连接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        while (true){
            connector.connect();

            connector.subscribe("gmall2021.*");

            Message message = connector.get(100);

            List<CanalEntry.Entry> entryList = message.getEntries();

            if (entryList.isEmpty()){
                System.out.println("没有数据了......");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }else {
                for (CanalEntry.Entry entry : entryList) {
                    String tableName = entry.getHeader().getTableName();

                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())){
                        ByteString storeValue = entry.getStoreValue();

                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        CanalEntry.EventType eventType = rowChange.getEventType();

                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        handler(tableName, eventType, rowDatasList);


                    }
                }
            }

        }
    }

    /**
     * 处理数据
     * @param tableName
     * @param eventType
     * @param rowDatasList
     */
    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        if ("order_info".equals(tableName)&& CanalEntry.EventType.INSERT.equals(eventType)){
            savetoKafka(GmallConstants.KAFKA_TOPIC_ORDER,rowDatasList);
        } else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            savetoKafka(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL,rowDatasList);
        } else if ("user_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            savetoKafka(GmallConstants.KAFKA_TOPIC_USER,rowDatasList);
        }

    }

    /**
     * 发送到对应的topic
     * @param topic
     * @param rowDatasList
     */
    private static void savetoKafka(String topic,List<CanalEntry.RowData> rowDatasList){
        for (CanalEntry.RowData rowData : rowDatasList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                jsonObject.put(column.getName(), column.getValue());
            }
            try {
                Thread.sleep(new Random().nextInt(3)*100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            System.out.println(jsonObject);
            MyKafkaUtils.sender(topic,jsonObject.toString());
        }
    }
}
