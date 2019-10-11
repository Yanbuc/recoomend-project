package constant;

// 使用接口之中的变量 存储着需要的常量值。
public interface Constants {

    // 项目运行的模式
    String LOCAL="local";



    // 项目配置文件的名字
    String CONFIG_FILE_NAME="conf.properties";
    // 项目的名字
    String APP_NAME="appName";
    //################# 数据库相关的常量###############################
    String DRIVER_NAME="driver_name";
    String LOCAL_DB_URL="local_db_url";
    String LOCAL_DB_USER="local_db_user";
    String LOCAL_DB_PASSWORD="local_db_password";
    String PRODUCT_DB_URL="product_db_url";
    String PRODUCT_DB_USER="product_db_user";
    String PRODUCT_DB_PASSWORD="product_db_pwd";

    //##################################### 任务筛选的条件常量
    String CONDITIONS="startDate,endDate,startAge,endAge";
    String START_DATE="startDate";
    String END_DATE="endDate";
    String START_AGE="startAge";
    String END_AGE="endAge";
    String SEARCH_KEY_WORD="searchKeyWord";
    String SESSION_ID="session_id";
    String CLICK_CATEGORY_IDS="clickCategoryIds";
    String CITY="city";
    String SEX="sex";
    String PROFESSION="professional";
    String SESSION_TIME="sessionTime";
    String STEP_LENGTH="stepLength";
    String START_TIME="startTime";
    String ORDER_CATEGORY_IDS="orderCategoryIds";
    String PAY_CATEGORY_IDS="payCategoryIds";
    String PAY_CATGEORY_COUNT="payCategoryCount";
    String ORDER_CATEGORY_COUNT="orderCategoryCount";
    String CLICK_CATEGORY_COUNT="clickCategoryCount";
    String CITY_NAME="cityName";
    String AREA_NAME="areaName";
    String CITY_ID="cityId";
    String PRODUCT_ID="productId";

    //##############表格字段
    String FIELD_USER_ID="userId";
    String FIELD_USERNAME="username";
    String FIELD_NAME="name";
    String FILED_AGE="age";
    String FIELD_PROFRESSION="profressional";
    String FIELD_CITY="city";
    String FIELD_SEX="sex";


    //累加器之中运用到的常量
    String SESSION_COUNT="session_count";
    String TIME_PERIOD_1s_3s="time_period_1s_3s";
    String TIME_PERIOD_4s_6s="time_period_4s_6s";
    String TIME_PERIOD_7s_9s="time_period_7s_9s";
    String TIME_PERIOD_10s_30s="time_period_10s_30s";
    String TIME_PERIOD_30s_60s="time_period_30s_60s";
    String TIME_PERIOD_1m_3m="time_period_1m_3m";
    String TIME_PERIOD_3m_10m="time_period_3m_10m";
    String TIME_PERIOD_10m_30m="time_period_10m_30m";
    String TIME_PERIOD_30m="time_period_30m";
    String STEP_PERIOD_1_3="step_period_1_3";
    String STEP_PERIOD_4_6="step_period_4_6";
    String STEP_PERIOD_7_9="step_period_7_9";
    String STEP_PERIOD_10_30="step_period_10_30";
    String STEP_PERIOD_30_60="step_period_30_60";
    String STEP_PERIOD_60="step_period_60";

    String TAEGET_PAGE_FLOW="targetPageFlow";


}
