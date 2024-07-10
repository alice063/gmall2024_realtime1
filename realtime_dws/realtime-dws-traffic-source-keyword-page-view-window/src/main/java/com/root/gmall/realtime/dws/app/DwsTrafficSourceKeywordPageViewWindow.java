package com.root.gmall.realtime.dws.app;

import com.root.gmall.realtime.common.base.BaseSQLApp;
import com.root.gmall.realtime.common.constant.Constant;
import com.root.gmall.realtime.common.util.SQLUtil;
import com.root.gmall.realtime.dws.function.KwSplit;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {

    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(10021,4,"dws_traffic_source_keyword_page_view_window");
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String ckAndGroupId) {
        //1.读取主流DWD页面主题数据
        tableEnv.executeSql("create table page_info("+
                "`common` map<STRING,STRING>,"+
                "`page` map<STRING,STRING>,"+
                "`ts` bigint,"+
                "`row_time` as TO_TIMESTAMP_LTZ(ts,3),"+
                "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND"+
                ")"+ SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRAFFIC_PAGE,ckAndGroupId));
        //2.筛选关键字
        Table keywordsTable =tableEnv.sqlQuery("select " +
                "page['item'] keywords," +
                "`row_time` " +
                "from page_info " +
                "where page['last_page_id']='search' " +
                "and page['item_type']='keyword' " +
                "and page['item'] is not null");
        tableEnv.createTemporaryView("keywords_table",keywordsTable);
        //3.自定义UDTF分词函数并注册
        tableEnv.createTemporarySystemFunction("KwSplit", KwSplit.class);
        //4.调用分词函数
        Table keywordTable = tableEnv.sqlQuery("SELECT keywords,keyword,`row_time` " +
                "FROM keywords_table " +
                "LEFT JOIN LATERAL TABLE(KwSplit(keywords)) ON TRUE");
        tableEnv.createTemporaryView("keyword_table",keywordTable);
        //5.对关键词进行分组开窗聚合
        Table windowAggTable = tableEnv.sqlQuery("SELECT " +
                "cast(TUMBLE_START(row_time,INTERVAL '10' SECOND) as STRING) stt," +
                "cast(TUMBLE_END(row_time,INTERVAL '10' SECOND) as STRING) edt," +
                "cast(CURRENT_DATE as STRING) cur_date," +
                "keyword," +
                "count(*) keyword_count " +
                "FROM keyword_table " +
                "GROUP BY " +
                "TUMBLE(row_time,INTERVAL '10' SECOND), " +
                "keyword");
        //6.写出到doris
        //开启检查点
        tableEnv.executeSql("CREATE TABLE doris_sink ("+
                "stt STRING,"+
                "edt STRING,"+
                "cur_date STRING,"+
                "`keyword` STRING,"+
                "keyword_count bigint"+
                ")"+SQLUtil.getDorisSinkSSQL(Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW));
        windowAggTable.insertInto("doris_sink").execute();
    }
}

//运行不成功  =》  设置并行度？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？
