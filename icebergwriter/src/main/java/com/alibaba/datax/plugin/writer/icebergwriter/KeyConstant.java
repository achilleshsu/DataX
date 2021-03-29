package com.alibaba.datax.plugin.writer.icebergwriter;

public class KeyConstant {
    /**
     * s3 儲存體的地址，需要包含 bucketName 
     */
    public static final String ENDPOINT = "endpoint";
    /**
     * s3 儲存體的 accessKey
     */
    public static final String ACCESS_KEY = "accessKey";
    /**
     * s3 儲存體的 secretKey
     */
    public static final String SECRET_KEY = "secretKey";
    /**
     * s3 儲存體的 databaseName
     */
    public static final String DATABASE_NAME = "dbName";
    /**
     * s3 儲存體的 tableName
     */
    public static final String TABLE_NAME = "tableName";
    /**
     * 傳入的列
     */
    public static final String COLUMN = "column";
    /**
     * 每个列的名字
     */
    public static final String COLUMN_NAME = "name";
    /**
     * 每个列的類型
     */
    public static final String COLUMN_TYPE = "type";
    /**
     * 每個元素的類型
     */
    public static final String ITEM_TYPE = "itemtype";
    /**
     * 列分隔符
     */
    public static final String COLUMN_SPLITTER = "splitter";
    /**
     * 數據更新訊息
     */
    public static final String WRITE_MODE = "writeMode";
    /**
     * 有相同的紀錄是否覆蓋，默認為false
     */
    public static final String IS_REPLACE = "isUpsert";
    /**
     * 指定用來判斷是否覆蓋的 主鍵
     */
    public static final String UNIQUE_KEY = "upsertKey";
    /**
     * 陣列類型
     */
    public static final String ARRAY_TYPE = "array";
    /**
     * ObjectId 類型
     */
    public static final String OBJECT_ID_TYPE = "objectid";
    
    /**
     * 判斷是否為數組類型
     * @param type 數據類型
     * @return
     */
    public static boolean isArrayType(String type) {
        return ARRAY_TYPE.equals(type);
    }
    /**
     * 判斷是否為ObjectId類型
     * @param type 數據類型
     * @return
     */
    public static boolean isObjectIdType(String type) {
        return OBJECT_ID_TYPE.equals(type);
    }
    /**
     * 判斷一個值是否為true
     * @param value
     * @return
     */
    public static boolean isValueTrue(String value){
        return "true".equals(value);
    }
}
