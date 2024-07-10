package com.root.gmall.realtime.dwd.db.app;

import com.root.gmall.realtime.common.base.BaseSQLApp;
import com.root.gmall.realtime.common.constant.Constant;
import com.root.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderPaySucDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(10016,4,"dwd_trade_order_pay_suc_detail");
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String ckAndGroupId) {
        //1.读取topic_db数据
        createTopicDb(ckAndGroupId,tableEnv);
        //2.筛选支付成功的数据
        Table paymentTable = tableEnv.sqlQuery("select " +
                "data['id'] id," +
                "data['order_id'] order_id," +
                "data['user_id'] user_id," +
                "data['payment_type'] payment_type," +
                "data['total_amount'] total_amount," +
                "data['callback_time'] callback_time," +
                "ts," +
                "row_time," +
                "proc_time " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='payment_info' " +
                "and `type`='update' " +
                "and `old`['payment_status'] is not null " +
                "and `data`['payment_status']='1602'");
        tableEnv.createTemporaryView("payment",paymentTable);
        //3.读取下单详情表数据
        tableEnv.executeSql("create table order_detail (" +
                "id string," +
                "order_id string," +
                "sku_id string," +
                "user_id string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "sku_name string," +
                "order_price string," +
                "sku_num string," +
                "create_time string," +
                "split_total_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "ts bigint," +
                "row_time as TO_TIMESTAMP_LTZ(ts*1000,3)," +
                "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND" +
                ")"+ SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,ckAndGroupId));
        //4.创建base_dic字典表
        createBaseDic(tableEnv);
        //5.使用interval join 完成两个流的数据关联
        Table payOrderTable = tableEnv.sqlQuery("SELECT " +
                "od.id," +
                "p.order_id," +
                "p.user_id," +
                "payment_type," +
                "callback_time payment_time," +
                "sku_id," +
                "province_id," +
                "activity_id," +
                "activity_rule_id," +
                "coupon_id," +
                "sku_name," +
                "order_price," +
                "sku_num," +
                "split_total_amount," +
                "split_activity_amount," +
                "split_coupon_amount," +
                "p.ts," +
                "p.proc_time " +
                "FROM payment p, order_detail od " +
                "WHERE p.order_id=od.order_id " +
                "AND p.row_time BETWEEN od.row_time - INTERVAL '15' MINUTE AND od.row_time + INTERVAL '5' SECOND");
        tableEnv.createTemporaryView("pay_order",payOrderTable);
        //使用lookup join完成维度退化
        Table resultTable = tableEnv.sqlQuery("SELECT " +
                "id," +
                "order_id," +
                "user_id," +
                "payment_type payment_type_code," +
                "info.dic_name payment_type_name," +
                "payment_time," +
                "sku_id," +
                "province_id," +
                "activity_id," +
                "activity_rule_id," +
                "coupon_id," +
                "sku_name," +
                "order_price," +
                "sku_num," +
                "split_total_amount," +
                "split_activity_amount," +
                "split_coupon_amount," +
                "ts " +
                "FROM pay_order p " +
                "left join base_dic FOR SYSTEM_TIME AS OF p.proc_time as b " +
                "on p.payment_type = b.rowkey");
        //创建upsert kafka
        tableEnv.executeSql("create table dwd_trade_order_payment_success (" +
                "id string," +
                "order_id string," +
                "user_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "payment_time string," +
                "sku_id string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "sku_name string," +
                "order_price string," +
                "sku_num string," +
                "split_total_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "ts bigint," +
                "PRIMARY KEY (id) NOT ENFORCED" +
                ")"+SQLUtil.getUpsertKafkaSQL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));
        resultTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS).execute();

    }
}
