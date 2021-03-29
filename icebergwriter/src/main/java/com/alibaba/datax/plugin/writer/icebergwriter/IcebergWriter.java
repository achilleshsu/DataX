package com.alibaba.datax.plugin.writer.icebergwriter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
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
	
//	public static void main( String[] args ) {
//    	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    	StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//    	
//		env.setParallelism(1);
//		env.enableCheckpointing(5000L);
//    	System.setProperty("HADOOP_USER_NAME", "root");
//    	System.out.println("creat catalog start");
//    	
//		tableEnv.executeSql("CREATE CATALOG hadoop_catalog WITH (\n" + 
//				"'type'='iceberg',\n" + 
//				"'catalog-type'='hadoop',\n" + 
//				"'warehouse'='" + "alluxio://172.22.23.156:19998/achillestest" + "',\n" + 
//				"'property-version'='1'\n" + 
//				")");
//		
//		
//		System.out.println("creat catalog end");
//		TableResult tableResult2 = tableEnv.sqlQuery("select * from hadoop_catalog.demo.test").execute();
//		tableResult2.print();
//		System.out.println( "complete?!" );
//	}

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
	        	String isUpsert = this.originalConfig.getString(KeyConstant.IS_REPLACE);
	        	String upsertKey = this.originalConfig.getString(KeyConstant.UNIQUE_KEY);
	        	
	        	System.out.println("endpoint: " + endpoint);
	        	System.out.println("accessKey: " + accessKey);
	        	System.out.println("secretKey: " + secretKey);
	        	System.out.println("databaseName: " + databaseName);
	        	System.out.println("tableName: " + tableName);
	        	System.out.println("isUpsert: " + isUpsert);
	        	System.out.println("upsertkey: " + upsertKey);
	        	
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
	        private String isUpsert = null;
	        private String upsertKey = null;
	        private Table table = null;
	        private Integer batchSize = 1000;
	        private String sqlInsert = null;
	        private JSONArray icebergColumnMeta = null;
	        private JSONObject writeMode = null;
	        
	    	private StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	    	private StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

	        //吃參數，建立連線
	    	//這裡應該要設定好寫入是覆蓋還是取代，如果是取代的話 key 是什麼?? 
	        @Override
	        public void init() {
	            this.writerSliceConfig = super.getPluginJobConf();
	            
	        	this.endpoint = this.writerSliceConfig.getString(KeyConstant.ENDPOINT);
	        	this.accessKey = this.writerSliceConfig.getString(KeyConstant.ACCESS_KEY);
	        	this.secretKey = this.writerSliceConfig.getString(KeyConstant.SECRET_KEY);
	        	this.databaseName = this.writerSliceConfig.getString(KeyConstant.DATABASE_NAME); 
	        	this.tableName = this.writerSliceConfig.getString(KeyConstant.TABLE_NAME);
	        	this.isUpsert = this.writerSliceConfig.getString(KeyConstant.IS_REPLACE);
	        	this.upsertKey = this.writerSliceConfig.getString(KeyConstant.UNIQUE_KEY);
	        	this.icebergColumnMeta = JSON.parseArray(writerSliceConfig.getString(KeyConstant.COLUMN));
	        	this.writeMode = JSON.parseObject(writerSliceConfig.getString(KeyConstant.WRITE_MODE));

	        	//建立 iceberg catalog 
				env.setParallelism(1);
				env.enableCheckpointing(5000L);
	        	System.setProperty("HADOOP_USER_NAME", "root");
	        	System.out.println("creat catalog start");
	        	
	    		tableEnv.executeSql("CREATE CATALOG hadoop_catalog WITH (\n" + 
	    				"'type'='iceberg',\n" + 
	    				"'catalog-type'='hadoop',\n" + 
	    				"'warehouse'='" + endpoint + "',\n" + 
	    				"'property-version'='1'\n" + 
	    				")");
	    		
	    		
	    		System.out.println("creat catalog end");
	        }

	        //在準備階段中檢查
	        //1. 檢查DB，不存在幫建
	        //2. 檢查TABLE,不存在幫建 (暫時註解，因為還沒有IF NOT EXISTS功能)	        
	        @Override
	        public void prepare() {
	        	//1. 檢查DB，不存在幫建
	        	System.out.println("check database start");
	        	tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS hadoop_catalog." + this.databaseName);
	        	System.out.println("check database end");
	        	
	        	//2. 檢查TABLE，存在要跳出，不存在一律幫建
	        	//tableEnv.executeSql("CREATE TABLE hadoop_catalog.demo.test2 (`id` BIGINT,`data` STRING)");
	        	System.out.println("check table start");
	        	//取出相對應的型態
//	        	String createTableSql = preparedCreateTableSql(this.icebergColumnMeta);
//	        	System.out.println("createTableSql:" + createTableSql);
//	        	try {
//	        		tableEnv.executeSql("CREATE TABLE hadoop_catalog." + this.databaseName + "." + this.tableName + " (" + createTableSql + ")");
//	        	} catch (Exception e){
//	    			throw DataXException
//                    .asDataXException(
//                    		IcebergWriterErrorCode.UNEXCEPT_EXCEPTION,
//                            String.format(
//                            		"出現預期之外的錯誤，請檢查表格是否已存在，若存在請刪除.")
//                            );
//	        	}
	        	System.out.println("check table end");
	        }

	        
	        public void startWrite(RecordReceiver lineReceiver) {
	    		//每次只讀這樣
	        	List<Record> writerBuffer = new ArrayList<Record>(this.batchSize);
	        	Record record = null;
	        	System.out.println("start write");
	        	
	            while((record = lineReceiver.getFromReader()) != null) {
	                writerBuffer.add(record);
	                if(writerBuffer.size() >= this.batchSize) {
	                    doBatchInsert(tableEnv,writerBuffer);
	                    writerBuffer.clear();
	                }
	            }
	            if(!writerBuffer.isEmpty()) {
	                doBatchInsert(tableEnv,writerBuffer);
	                writerBuffer.clear();
	            }
	            System.out.println("write end");
	        }
	        
	      
	        //這裡可以多一些檢查機制
	        //比如說 schema 數量，schema 型態
	        private void doBatchInsert(StreamTableEnvironment tableEnv, List<Record> writerBuffer) {
	        	
	        	StatementSet stmtSet = tableEnv.createStatementSet();
	        	
	        	for(Record record : writerBuffer) {
	        		String insertSql = "";
	        		for(int i = 0; i < record.getColumnNumber(); i++) {
	        			String insertSchema = "";
	        			String tempInput = prepareCreateInsertSql(i, record.getColumn(i), icebergColumnMeta);
	        			if (i == 0) {
	        				insertSchema +=  tempInput;
	        			} else {
	        				insertSchema +=  "," + tempInput;
	        			}
	        		}
	        		
	        		String executeSql = "INSERT INTO hadoop_catalog." + this.databaseName + "." + this.tableName + " VALUES (" + insertSql + ")";
	        		stmtSet.addInsertSql(executeSql);
	        		System.out.println("executeSql: " + executeSql);
	        	}
	        	
	        	try {
		        	stmtSet.execute();
	        	}catch (Exception e) {
	    			throw DataXException
                  .asDataXException(
                  		IcebergWriterErrorCode.UNEXCEPT_EXCEPTION,
                          String.format(
                          		"出現預期之外的錯誤，錯誤訊息：" + e.toString())
                          );	        		
	        	}
	        }

	        @Override
	        public void post() {

	        }

	        @Override
	        public void destroy() {

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
	    
	    	private static String prepareCreateInsertSql(int Index, Column column, JSONArray icebergColumnMeta) {
	    		String insertSql = "";
	    		
	    		JSONObject icebergcloumn = icebergColumnMeta.getJSONObject(Index);
	    		
	    		switch(icebergcloumn.getString("type")) {
    			case "BOOLEAN":
    			case "VARCHAR":
    			case "VARBINARY":
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
    				insertSql = "'" + column.asString() + "'";
    				break;
    			case "TINYINT":
    			case "SMALLINT":
    			case "INT":
    			case "BIGINT":
    			case "FLOAT":
    			case "DOUBLE":
    			case "DECIMAL":
    				insertSql = column.asString();
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
