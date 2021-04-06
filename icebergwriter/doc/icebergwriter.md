# DataX IcebergWriter 插件文檔


------------

## 1 快速介紹

IcebergWriter提供向S3文件系統指定路徑中寫入parquet文件。


## 2 功能與限制

* (1)、目前IcebergWriter僅支持parquet格式的文件，且文件內容存放的必須是一張表；
* (2)、目前版本是透過 spark sql 寫入資料湖，因此有些類型的 schema 尚未支持；
* (3)、目前僅支持與以下Iceberg數據類型：<br/>
  數值型：TINYINT，SMALLINT，INT，INTEGER，BIGINT，FLOAT，DOUBLE，DECIMAL<br/>
  字符串類型：CHAR，VARCHAR，STRING<br/>
  布爾類型：布爾類型：BOOLEAN<br/>
  時間類型：DATE，TIMESTAMP<br/>
  **目前不支持：binary、arrays、maps、structs、union類型**;

* (4)、目前只支援 hdfs 和 alluxio 寫入，S3寫入待開發
* (5)、IcebergWriter實現過程是：需要用戶先將欲寫入的表格準備完成，準備完成之後才開始。

## 3 功能說明


### 3.1 配置樣例

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 3,
        "byte": 1048576
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0.02
      }
    },
    "content": [
      {
        "reader": {
          "name": "postgresqlreader",
          "parameter": {
            "username": "username",
            "password": "password",
            "column": [
              "\"c_id\"",
              "\"content\""
            ],
            "splitPk": "",
            "connection": [
              {
                "table": [
                  "public.read1"
                ],
                "jdbcUrl": [
                  "jdbc:postgresql://61.219.26.55:5432/postgres"
                ]
              }
            ]
          }
        },
        "writer": {
          "name": "icebergwriter",
          "parameter": {
            "endpoint": "alluxio://172.22.23.156:19998/achillestest",
            "accessKey": "",
            "secretKey": "",
            "dbName": "demo",
            "tableName": "test",
            "column": [
              {
                "name": "id",
                "type": "INT"
              },
              {
                "name": "data",
                "type": "STRING"
              }
            ]
          }
        }
      }
    ]
  }
}

```

### 3.2 參數說明

* **endpoint**

	* 描述：S3 儲存體的位置，需要帶上 bucket，舉例： http://x.x.x.x:8889/\<bucketName\> <br />

	* 必選：是 <br />

	* 默認值：無 <br />

* **accessKey**

	* 描述：登入 S3 位置的 access Key 通常是一組亂數 <br />

	* 必選：否<br />

	* 默認值：無<br />

* **secretKey**

	* 描述：登入 S3 位置的 secret Key 通常是一組亂數 <br />

	* 必選：否<br />

	* 默認值：無<br />

* **dbName**

	* 描述：Database 的名稱，不同於 bucket name ，一個endpoint 可以有數個 database name <br />

	* 必選：是 <br />

	* 默認值：無 <br />

* **tableName**

	* 描述： Table的名稱，不同於 database name ，一個database 可以有數個 table name<br />

	* 必選：是 <br />

	* 默認值：無 <br />

* **column**

	* 描述：表個中的欄位，一個 table 可以有數個欄位，一定至少要有一組 <br />

	* 必選：是 <br />

	* 默認值：無 <br />

* **column.name**

	* 描述：表個欄位的名稱 <br />

	* 必選：是<br />

	* 默認值：無<br />

* **column.type**

	* 描述：表格欄位中資料的型態，儲存的時候會用到 <br />

	* 必選：是<br />

	* 默認值：無<br />


## 3.3 類型轉換

目前 IcebergWriter 支持大部分類型，請注意檢查你的類型。

下面列出 IcebergWriter 針對 Datax 數據類型轉換列表:

| DataX 內部類型| IcebergWriter 數據類型 |
| -------- | ----- |
| Long | TINYINT,SMALLINT,INTEGER,BIGINT,INT|
| Double | DECIMAL,FLOAT,DOUBLE|
| String | CHAR,VARCHAR,STRING |
| Boolean | BOOLEAN|
| Date | DATE,TIMESTAMP|
| Byte | BYTES|

## 4 配置步驟
* 步驟一、在 Iceberg 中創建數據庫、表

* 步驟二、根據步驟一的配置信息配置IcebergWriter作業

## 5 約束限制

略

## 6 FAQ

略
