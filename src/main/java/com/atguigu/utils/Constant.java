package com.atguigu.utils;

public class Constant {
    public static final String KAFKA_BROKERS = "localhost:9092";
    public static final String TOPIC_DB = "topic_db";
    public static final String TOPIC_LOG = "topic_log";

    public static final String DIM_DATABASE = "dim_database";
    public static final String DIM_TABLE = "dim_table";

    public static final String MYSQL_HOST = "localhost";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "123456";
    public static final String HBASE_NAMESPACE = "imedw";

    public static final String MYSQL_DRIVER = com.mysql.jdbc.Driver.class.getName();
    public static final String MYSQL_URL = "jdbc:mysql://localhost:3306?useSSL=false";

    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start_log";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err_log";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page_log";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action_log";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display_log";

    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";

    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";

    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel_detail";

    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_pay_suc_detail";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";

    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_trade_refund_pay_suc_detail";

    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";

    public static final String DORIS_FE_NODES = "localhost:7030";

    public static final String DORIS_DATABASE = "gmall2023_realtime";

    public static final int TWO_DAY_SECONDS = 2 * 24 * 60 * 60;

    public static final String SR_JDBC_URL = "SR_JDBC_URL";
    public static final String SR_LOAD_URL = "SR_LOAD_URL";
    public static final String SR_USERNAME = "SR_USERNAME";
    public static final String SR_PASSWORD = "SR_PASSWORD";
    public static final String SINK_TEST_DRIVER_TABLE = "SINK_TEST_DRIVER_TABLE";
    public static final String DATABASE_NAME = "DATABASE_NAME";
    public static final String FLUSH_INTERVAL = "FLUSH_INTERVAL";
}
