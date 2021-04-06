package com.alibaba.datax.plugin.writer.icebergwriter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.http.conn.ssl.BrowserCompatHostnameVerifier;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;

public class IcebergWriter extends Writer {
	
	 public static class Job extends Writer.Job {
		 private static final Logger LOG = LoggerFactory.getLogger(Job.class);
		 private Configuration originalConfig = null;
		 
		 	@Override
		 	public void preCheck() {
		 		init();
		 	}
		 	        
		 	//這段做了 讀取 config 解析判斷，沒有嘗試連線
		 	// iceberg 目前可以接上一般S3, Alyun oss 和 alluxio，minio 屬於 S3 的一種
		 	// 目前現階段做的是只有 alluxio 
		 	// S3 預計完成 alluxio 後會加上去 
		
	        @Override
	        public void init() {
	            // 取得 wirter 的 config 		    	
	        	this.originalConfig = super.getPluginJobConf();
	        	
	        	String endpoint = this.originalConfig.getString(KeyConstant.ENDPOINT);
	        	String accessKey = this.originalConfig.getString(KeyConstant.ACCESS_KEY);
	        	String secretKey = this.originalConfig.getString(KeyConstant.SECRET_KEY);
	        	String databaseName = this.originalConfig.getString(KeyConstant.DATABASE_NAME); 
	        	String tableName = this.originalConfig.getString(KeyConstant.TABLE_NAME);

//	        	System.out.println("endpoint: " + endpoint);
//	        	System.out.println("accessKey: " + accessKey);
//	        	System.out.println("secretKey: " + secretKey);
//	        	System.out.println("databaseName: " + databaseName);
//	        	System.out.println("tableName: " + tableName);

	        	//測試使用
//	        	System.out.println("creat catalog start");
//	        	//建立 iceberg catalog 
//	    		SparkConf sparkConf = new SparkConf();
//	    		sparkConf.setAppName("test");
//	    		sparkConf.setMaster("local");
//	    		sparkConf.set("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog");
//	    		sparkConf.set("spark.sql.catalog.local.type","hadoop");
//	    		sparkConf.set("spark.sql.catalog.local.warehouse",endpoint);
//	    		sparkConf.set("spark.sql.warehouse.dir",endpoint);
//	    		System.out.println("creat catalog end");
	    		
//	    		System.out.println("connect iceberg start");
//	    		SparkSession spark = SparkSession
//	    			  .builder()
//	    			  .config(sparkConf)
//	    			  .getOrCreate();
//	    		System.out.println("connect iceberg end ");
//	    		System.out.println("insert table start");
//	    		spark.sql("INSERT INTO  local.demo.test VALUES (4, 'ddd')");
//	    		System.out.println("insert table stop");
//	    		spark.stop();  	
	        	
	        }
	               
	        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
	        @Override
	        public void prepare() {

	        }
	        
	        // 將 config 按照數量分下去
	        @Override
	        public List<Configuration> split(int mandatoryNumber) {
	            List<Configuration> configList = new ArrayList<Configuration>();
	            
	            for(int i = 0; i < mandatoryNumber; i++) {
	                configList.add(this.originalConfig.clone());
	            }
	            
	            return configList;
	        }
	        
	        // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
	        @Override
	        public void post() {

	        }
	        
	        @Override
	        public void destroy() {

	        } 
	 }
	 
	 
	    public static class Task extends Writer.Task {
	        private Configuration writerSliceConfig;
	        
	        private String endpoint = null;
	        private String accessKey = null;
	        private String secretKey = null;
	        private String databaseName = null;
	        private String tableName = null;
	        private Integer batchSize = 1000;
	        private JSONArray icebergColumnMeta =null;
	        
	        private SparkSession spark = null;
	        
	        //吃參數，建立連線
	    	//這裡應該要設定好寫入是覆蓋還是取代，如果是取代的話 key 是什麼?? 
        	//這邊要做出來不同 S3 的處理機制
	        @Override
	        public void init() {
	            this.writerSliceConfig = super.getPluginJobConf();
	            
	        	this.endpoint = this.writerSliceConfig.getString(KeyConstant.ENDPOINT);
	        	this.accessKey = this.writerSliceConfig.getString(KeyConstant.ACCESS_KEY);
	        	this.secretKey = this.writerSliceConfig.getString(KeyConstant.SECRET_KEY);
	        	this.databaseName = this.writerSliceConfig.getString(KeyConstant.DATABASE_NAME); 
	        	this.tableName = this.writerSliceConfig.getString(KeyConstant.TABLE_NAME);
	        	this.icebergColumnMeta = JSON.parseArray(writerSliceConfig.getString(KeyConstant.COLUMN));
       	
	        	//建立 iceberg catalog 

	        	System.out.println("creat catalog start");
	        	
	    		SparkConf sparkConf = new SparkConf();
	    		sparkConf.setAppName("test");
	    		sparkConf.setMaster("local");
	    		sparkConf.set("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog");
	    		sparkConf.set("spark.sql.catalog.local.type","hadoop");
	    		sparkConf.set("spark.sql.catalog.local.warehouse", this.endpoint);
	    		sparkConf.set("spark.sql.warehouse.dir", this.endpoint);

	    		System.out.println("creat catalog connection");
	    		this.spark = SparkSession
		    			  .builder()
		    			  .config(sparkConf)
		    			  .getOrCreate();
	    		System.out.println("creat catalog end");
	    		
	        }

	        //在準備階段中檢查
	        //1. 檢查DB，不存在幫建
	        //2. 檢查TABLE,不存在幫建 (暫時註解，因為建立表格細節挺多的，還不知道怎麼處理)	 
	        //3. 檢查SCHEMA，檢查輸入輸出的數量有沒有正確，
	        @Override
	        public void prepare() {
	        		        	
	        	//1. 檢查DB，不存在幫建
	        	System.out.println("check database start");
	        	this.spark.sql("CREATE DATABASE IF NOT EXISTS local." + this.databaseName);
	        	System.out.println("check database end");
	        	
	        	//2. 檢查TABLE，存在要跳出，不存在一律幫建
	        	//tableEnv.executeSql("CREATE TABLE hadoop_catalog.demo.test2 (`id` BIGINT,`data` STRING)");
//	        	System.out.println("check table start");
	        	//取出相對應的型態
//	        	String createTableSql = prepareCreateTableSql(this.icebergColumnMeta);
//	        	System.out.println("createTableSql:" + createTableSql);
//	        	try {
//	        		this.spark.sql("CREATE TABLE IF NOT EXISTS local." + this.databaseName + "." + this.tableName + " (" + createTableSql + ")");
//	        	} catch (Exception e){
//	    			throw DataXException
//                    .asDataXException(
//                    		IcebergWriterErrorCode.UNEXCEPT_EXCEPTION,
//                            String.format(
//                            		"出現預期之外的錯誤，請檢查表格是否已存在，若存在請刪除.")
//                            );
//	        	}
//	        	System.out.println("check table end");
	        	System.out.println("check schema start");
	        	//查詢目標表格，取得型態和數量
	        	int schemaCount = 0;
	        	try {
	        		Dataset<Row> df = this.spark.sql("select * from local." + databaseName + "." + tableName + " limit 0");
	        		schemaCount = df.schema().length();
	        	} catch (Exception e){
	        		throw DataXException
	        			.asDataXException(
	        				IcebergWriterErrorCode.UNEXCEPT_EXCEPTION,
	        				String.format(
	        						"出現預期之外的錯誤，請檢查表格是否已存在.")
	        				);
	        	}
	        	
	        	//比較數量
	        	if (this.icebergColumnMeta.size() == 0) {
	        		throw DataXException
        			.asDataXException(
        				IcebergWriterErrorCode.ILLEGAL_VALUE,
        				String.format(
        						"列配置信息有錯誤. 因為您配置的任務中，目標表的schema數量少於1，請檢查您的配置並作出修改.")
        				);
	        	} 
	        	else if (this.icebergColumnMeta.size() < schemaCount) {
	        		throw DataXException
        			.asDataXException(
        				IcebergWriterErrorCode.ILLEGAL_VALUE,
        				String.format(
        						"列配置信息有錯誤. 因為您配置的任務中，目標表的schema數量大於數據輸入schema數量，請檢查您的配置並作出修改.")
        				);	        		
	        	} else if (this.icebergColumnMeta.size() > schemaCount) {
	        		throw DataXException
        			.asDataXException(
        				IcebergWriterErrorCode.ILLEGAL_VALUE,
        				String.format(
        						"列配置信息有錯誤. 因為您配置的任務中，目標表的schema數量小於數據輸入schema數量，請檢查您的配置並作出修改.")
        				);	        		
	        	}
	        	
	        	System.out.println("check schema end");
	        }

	        
	        public void startWrite(RecordReceiver lineReceiver) {
	    		//每次只讀這樣
	        	List<Record> writerBuffer = new ArrayList<Record>(this.batchSize);
	        	Record record = null;
	        	System.out.println("start write");
	        	
	            while((record = lineReceiver.getFromReader()) != null) {
	                writerBuffer.add(record);
	                if(writerBuffer.size() >= this.batchSize) {
	                    doBatchInsert(writerBuffer);
	                    writerBuffer.clear();
	                }
	            }
	            if(!writerBuffer.isEmpty()) {
	                doBatchInsert(writerBuffer);
	                writerBuffer.clear();
	            }
	            System.out.println("write end");
	        }
	        
	      
	        //這裡有檢查了型態
	        private void doBatchInsert(List<Record> writerBuffer) {

	        	for(Record record : writerBuffer) {
	        		String insertSql = "";
	        		for(int i = 0; i < record.getColumnNumber(); i++) {
	        			String tempInput = prepareCreateInsertSql(i, record.getColumn(i), this.icebergColumnMeta);
	        			if (i == 0) {
	        				insertSql +=  tempInput;
	        			} else {
	        				insertSql +=  "," + tempInput;
	        			}
	        		}
	        		
	        		String executeSql = "INSERT INTO local." + this.databaseName + "." + this.tableName + " VALUES (" + insertSql + ")";
	        		System.out.println("executeSql: " + executeSql);
	        		
		        	try {
		        		this.spark.sql(executeSql);
		        	}catch (Exception e) {
		        		System.out.println("exception e: " + e);
		        		System.out.println("more: " + e.getMessage());
		        		getTaskPluginCollector().collectDirtyRecord(record, "Put data points failed!");   
		        	}	
	        	}
	        }

	        @Override
	        public void post() {

	        }

	        @Override
	        public void destroy() {
	        	this.spark.stop();
	        }

	        @Override
	        public boolean supportFailOver(){
	        	return false;
	        }

	        //製作寫入表格
	        private String prepareCreateTableSql(JSONArray icebergColumnMeta) {
	        	
	        	String createTableSql = "";
	        	//遍歷JSONArray內容 
	        	for(int i = 0;i < icebergColumnMeta.size(); i++) {
	        		JSONObject columns = icebergColumnMeta.getJSONObject(i);
	        		String name = columns.getString("name");
	        		String type = columns.getString("type");
	        		
	        		switch(type) {
	        			case "BOOLEAN":
	        			case "TINYINT":
	        			case "SMALLINT":
	        			case "INT":
	        			case "BIGINT":
	        			case "FLOAT":
	        			case "DOUBLE":
	        			case "VARCHAR":
	        			case "VARBINARY":
	        			case "DECIMAL":
	        			case "DATE":
	        			case "TIME":
	        			case "TimestampType":
	        			case "LocalZonedTimestampType":
	        			case "INTERVAL YEAR TO MONTH":
	        			case "INTERVAL DAY TO SECOND":
	        			case "ARRAY":
	        			case "MULTISET":
	        			case "MAP":
	        			case "ROW":
	        				break;
	        			default:
	    	    			throw DataXException
	                        .asDataXException(
	                        		IcebergWriterErrorCode.UNEXCEPT_EXCEPTION,
	                                String.format(
	                                		"列配置信息有錯誤. 因為您配置的任務中，目標表的schema型態有誤. 請檢查您的配置並作出修改.")
	                                );	        				
	        		}
	        		
	        		if ( i == 0 ) {
	        			createTableSql += "`" + name + "` " + type;
	        		} else {
	        			createTableSql += ",`" + name + "` " + type;
	        		}
	        	}
	        	return createTableSql;
	        } 
	    	}
	    
	    	//提示要更清楚
			private static String prepareCreateInsertSql(int Index, Column column, JSONArray icebergColumnMeta) {
	    		String insertSql = "";
	    		
	    		JSONObject icebergcloumn = icebergColumnMeta.getJSONObject(Index);
	    		
	    		switch(icebergcloumn.getString("type")) {
	    		case "TINYINT":
	    		case "SMALLINT":
	    		case "INT":
	    		case "INTEGER":	
	    		case "BIGINT":
	    			insertSql = column.asString();
	    			break;
    			case "FLOAT":
    			case "DOUBLE":
    			case "DECIMAL":
    				insertSql = column.asString();
    				break;
    			case "BOOLEAN":
    				insertSql = "'" + column.asString() + "'";
    				break;
    			case "CHAR":
    			case "VARCHAR":
    			case "STRING":
    			case "ARRAY":
    			case "MULTISET":
    			case "MAP":
    			case "ROW":
    			case "RAW":
    			case "INTERVAL":	
    				insertSql = "'" + column.asString() + "'";
    				break;
    			case "DATE":
    				insertSql = "cast('" + column.asString() + "' as date)";
    				break;
    			case "TIMESTAMP":	
    				insertSql = "cast('" + column.asString() + "' as timestamp)";
    				break;
    			case "BYTES":
    				insertSql = "'" + column.asString() + "'";
    				break;
    			default:
	    			throw DataXException
                    .asDataXException(
                    		IcebergWriterErrorCode.UNEXCEPT_EXCEPTION,
                            String.format(
                            		"列配置信息有錯誤. 因為您配置的任務中，目標表的schema型態有誤. 請檢查您的配置並作出修改.")
                            );	  
	    		}
  

	    		return insertSql;
	    	}
	
}
