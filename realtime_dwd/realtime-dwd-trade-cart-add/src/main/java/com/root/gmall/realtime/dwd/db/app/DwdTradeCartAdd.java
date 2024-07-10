package com.root.gmall.realtime.dwd.db.app;

import com.root.gmall.realtime.common.base.BaseSQLApp;
import com.root.gmall.realtime.common.constant.Constant;
import com.root.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartAdd extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10013,4,"dwd_trade_cart_add");
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String ckAndGroupId) {
        //1.读取topic_db数据
        createTopicDb(ckAndGroupId,tableEnv);
        //2.筛选加购数据
        Table cartAddTable = tableEnv.sqlQuery("select " +
                " `data`['id'] id," +
                " `data`['user_id'] user_id," +
                " `data`['sku_id'] sku_id," +
                " `data`['cart_price'] cart_price," +
                " if(`type`='insert'," +
                "   cast(`data`['sku_num'] as int), " +
                "   cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int)" +
                ") sku_num ," +
                " `data`['sku_name'] sku_name," +
                " `data`['is_checked'] is_checked," +
                " `data`['create_time'] create_time," +
                " `data`['operate_time'] operate_time," +
                " `data`['is_ordered'] is_ordered," +
                " `data`['order_time'] order_time," +
                " `data`['source_type'] source_type," +
                " `data`['source_id'] source_id," +
                " ts " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='cart_info' " +
                "and (" +
                " `type`='insert' " +
                "  or(" +
                "     `type`='update' " +
                "      and `old`['sku_num'] is not null " +
                "      and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int) " +
                "   )" +
                ")");
        //3.创建kafka sink映射
        tableEnv.executeSql("create table dwd_trade_cart_add(" +
                "   id string, " +
                "   user_id string," +
                "   sku_id string," +
                "   cart_price string," +
                "   sku_num int, " +
                "   sku_name string," +
                "   is_checked string," +
                "   create_time string," +
                "   operate_time string," +
                "   is_ordered string," +
                "   order_time string," +
                "   source_type string," +
                "   source_id string," +
                "   ts  bigint " +
                ")"+ SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_CART_ADD));
        //4.写出筛选的数据到对应的kafka主题
        cartAddTable.insertInto(Constant.TOPIC_DWD_TRADE_CART_ADD).execute();
    }
}
