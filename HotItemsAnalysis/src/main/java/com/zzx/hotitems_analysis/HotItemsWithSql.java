package com.zzx.hotitems_analysis;

import com.zzx.hotitems_analysis.beans.UserBehavior;
import com.zzx.hotitems_analysis.udf_source.UserBehaviorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description: 实时热门商品统计
 * @data: 2021/4/6 17:06 PM
 */
public class HotItemsWithSql {
    public static void main(String[] args) throws Exception {

        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 将并行度设置为1 方便测试
        env.setParallelism(1);

        // 此参数不用设置，flink1.12版本默认是事件时间 EventTime
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 利用自定义数据源向 kafka 写入数据
        DataStreamSink<UserBehavior> outputDataStream = env
                .addSource(new UserBehaviorSource())
                .addSink(new FlinkKafkaProducer<UserBehavior>("localhost:9092",
                        "hotitems",
                        new TypeInformationSerializationSchema(TypeInformation.of(UserBehavior.class),
                                env.getConfig())));


        // kafka 连接配置
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // 从 kafka 读取数据,创建 DataStream ,并分配时间戳 和 Watermark
        DataStream<UserBehavior> dataStream = env
                .addSource(new FlinkKafkaConsumer<>("hotitems",
                        new TypeInformationSerializationSchema(TypeInformation.of(UserBehavior.class),
                                env.getConfig()),
                        props))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, l) -> event.getTimestamp() + 8 * 60 * 60 * 1000));

        // 创建表执行环境, 用 blink 版本
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 将流转换成表
        Table dataTable = tableEnv
                .fromDataStream(dataStream,
                        $("itemId"),
                        $("behavior"),
                        $("timestamp").rowtime().as("ts"));

        // 分组开窗
        // table API
        Table windowAggTable = dataTable
                .filter($("behavior").isEqual("pv"))
                .window(
                        Slide
                                .over(lit(1)
                                        .minutes())
                                .every(lit(5)
                                        .seconds())
                                .on($("ts"))
                                .as("w"))
                .groupBy($("itemId"), $("w"))
                .select($("itemId"),
                        $("w")
                                .end()
                                .as("windowEnd"),
                        $("itemId").count().as("cnt"));
//        windowAggTable.printSchema();

        // 利用开窗函数，对 count 值进行排序，并获取 Row number ，得到 TopN
        // SQL
        DataStream<Row> aggStream = tableEnv.toAppendStream(windowAggTable, Row.class);
        tableEnv
                .createTemporaryView("agg", aggStream,
                        $("itemId"),
                        $("windowEnd"),
                        $("cnt"));

        Table resultTable = tableEnv
                .sqlQuery("select itemId,DATE_FORMAT(windowEnd,'yyyy-MM-dd hh:mm:ss'),cnt,row_num from " +
                        "(select itemId,windowEnd,cnt, " +
                        "ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num " +
                        "from agg) " +
                        "where row_num <= 5");

        resultTable.printSchema();
        tableEnv.toRetractStream(resultTable, Row.class).print("result");

        // 纯 SQL 实现
        tableEnv
                .createTemporaryView("data_table",
                        dataStream,
                        $("itemId"),
                        $("behavior"),
                        $("timestamp").rowtime().as("ts"));
        Table resultSqlTable = tableEnv
                .sqlQuery(
                        "select itemId,windowEnd,cnt,row_num from ( " +
                        "   select itemId,windowEnd,cnt, " +
                        "   ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num " +
                        "   from ( " +
                        "       select itemId, count(itemId) as cnt, " +
                        "       DATE_FORMAT(HOP_END(ts, interval '5' seconds, interval '1' minutes ),'yyyy-MM-dd hh:mm:ss') as windowEnd " +
                        "       from data_table " +
                        "       where behavior = 'pv' " +
                        "       group by itemId,HOP(ts, interval '5' seconds, interval '1' minutes ) " +
                        "       ) " +
                        "   ) " +
                        "where row_num <= 5");
        resultSqlTable.printSchema();
        tableEnv.toRetractStream(resultSqlTable, Row.class).print("sql");

        env.execute("Hot Items With Sql");
    }
}
