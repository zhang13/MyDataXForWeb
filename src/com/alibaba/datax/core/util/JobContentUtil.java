package com.alibaba.datax.core.util;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.alibaba.datax.core.util.container.CoreConstant;
/**
 * 
 * @author 张超
 *
 */
public class JobContentUtil {

	/**
	 * 默认写入前执行语句，清空表
	 */
	public static String DEFAULT_PRE_SQL="truncate table %s";
	private String from_userName;
	private String from_pwd;
	private String from_jdbcUrl;
	private String from_db;
	private String to_userName;
	private String to_pwd;
	private String to_jdbcUrl;
	private String to_db;
	
	public JobContentUtil() {
		super();
		PropertiesReader propertiesReader = new PropertiesReader();
		from_userName = propertiesReader.getFromDBUserName();
		from_pwd = propertiesReader.getFromDBPwd();
		from_jdbcUrl = propertiesReader.getFromDBJdbcUrl();
		from_db = propertiesReader.getFromDB();
		to_userName=propertiesReader.getToDBUserName();
		to_pwd=propertiesReader.getToDBPwd();
		to_jdbcUrl = propertiesReader.getToDBJdbcUrl();
		to_db = propertiesReader.getToDB();
	}

	public JobContentUtil(String from_userName, String from_pwd,
			String from_jdbcUrl, String from_db, String to_userName, String to_pwd,
			String to_jdbcUrl,String to_db) {
		super();
		this.from_userName = from_userName;
		this.from_pwd = from_pwd;
		this.from_jdbcUrl = from_jdbcUrl;
		this.from_db = from_db;
		this.to_userName = to_userName;
		this.to_pwd = to_pwd;
		this.to_jdbcUrl = to_jdbcUrl;
		this.to_db = to_db;
	}

	/**
	 * 读取所有列，插入时会先清空表
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public String getJobJson(String tableName) throws IOException{
		String readerTemp = getReader(tableName,null,null,null);
		String writeTemp = getWriter(tableName,null,String.format(DEFAULT_PRE_SQL, tableName));
		return madeJobJsonStr(readerTemp, writeTemp);
    }
	
	/**
	 * 读取所有列，插入时不清空表
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public String getJobJsonWithOutPreSql(String tableName) throws IOException{
		String readerTemp = getReader(tableName,null,null,null);
		String writeTemp = getWriter(tableName,null,null);
		return madeJobJsonStr(readerTemp, writeTemp);
    }
	
	/**
	 * 读取输入列，插入时会先清空表
	 * @param tableName
	 * @param columns
	 * @return
	 * @throws IOException
	 */
	public String getJobJson(String tableName,String[] columns) throws IOException{
		String readerTemp = getReader(tableName,columns,null,null);
		String writeTemp = getWriter(tableName,columns,String.format(DEFAULT_PRE_SQL, tableName));
		return madeJobJsonStr(readerTemp, writeTemp);
    }
	
	/**
	 * 自由配置 列、插入前执行语句
	 * @param tableName
	 * @param columns
	 * @param preSql 插入钱执行的语句
	 * @return
	 * @throws IOException
	 */
	public String getJobJson(String tableName,String[] columns,String preSql) throws IOException{
		String readerTemp = getReader(tableName,columns,null,null);
		String writeTemp = getWriter(tableName,columns,preSql);
		return madeJobJsonStr(readerTemp, writeTemp);
    }
	
	
	/**
	 * 自由配置 列、插入前执行语句
	 * @param fromTableName
	 * @param toTableName
	 * @param columns
	 * @param preSql 插入钱执行的语句
	 * @return
	 * @throws IOException
	 */
	public String getJobJson(String fromTableName,String toTableName,String[] columns,String preSql) throws IOException{
		String readerTemp = getReader(fromTableName,columns,null,null);
		String writeTemp = getWriter(toTableName,columns,preSql);
		return madeJobJsonStr(readerTemp, writeTemp);
    }
	
	/**
	 * /**
	 * 自由配置 列、插入前执行语句
	 * @param fromTableName
	 * @param toTableName
	 * @param columns
	 * @param preSql 插入钱执行的语句
	 * @param where
	 * @param querySql
	 * @return
	 * @throws IOException
	 */
	public String getJobJson(String fromTableName,String toTableName,String[] columns,String preSql,String where,String querySql) throws IOException{
		String readerTemp = getReader(fromTableName,columns,where,querySql);
		String writeTemp = getWriter(toTableName,columns,preSql);
		return madeJobJsonStr(readerTemp, writeTemp);
	}
	
	/**
	 * 
	 * @param fromTableName
	 * @param toTableName
	 * @param columns
	 * @param preSql
	 * @param where
	 * @param querySql
	 * @return
	 * @throws IOException
	 */
	public String getJobJson(String fromTableName,String toTableName,String[] fromColumns,String[] toColumns,String where,String querySql,String preSql) throws IOException{
		String readerTemp = getReader(fromTableName,fromColumns,where,querySql);
		String writeTemp = getWriter(toTableName,toColumns,preSql);
		return madeJobJsonStr(readerTemp, writeTemp);
	}
	
	private StringBuffer getColumns(String[] columns) {
		StringBuffer resultColumnStr=new StringBuffer();
		for(int i=0;i<columns.length;i++){
			resultColumnStr.append("\"");
			resultColumnStr.append(columns[i]);
			resultColumnStr.append("\"");
			if(i<columns.length-1){
				resultColumnStr.append(",");
			}
		}
		return resultColumnStr;
	}

	private String madeJobJsonStr(String readerTemp, String writeTemp)
			throws IOException {
		String jobModel = FileUtils.readFileToString(new File(String.format("%s//job//job_model.json",
				CoreConstant.DATAX_HOME)));
		jobModel=jobModel.replace("\"reader\":", "\"reader\":"+readerTemp);
		jobModel=jobModel.replace("\"writer\":", "\"writer\":"+writeTemp);
		return jobModel;
	}

	private String getWriter(String tableName,String[] columns,String preSql) throws IOException {
		String writeTemp = FileUtils.readFileToString(new File(String.format("%s//%s//plugin_job_template.json",
				CoreConstant.DATAX_PLUGIN_WRITER_HOME,to_db)));
		writeTemp = writeTemp.replace("\"username\": \"\"", String.format("\"username\": \"%s\"", to_userName));
		writeTemp = writeTemp.replace("\"password\": \"\"", String.format("\"password\": \"%s\"", to_pwd));
		if(columns!=null){
			writeTemp = writeTemp.replace("\"column\": []", String.format("\"column\": [%s]", getColumns(columns)));
		}else{
			writeTemp = writeTemp.replace("\"column\": []", "\"column\": [\"*\"]");
		}
		writeTemp = writeTemp.replace("\"table\": []", String.format("\"table\": [\"%s\"]", tableName));
		writeTemp = writeTemp.replace("\"jdbcUrl\": \"\"", String.format("\"jdbcUrl\": \"%s\"", to_jdbcUrl));
		if(preSql!=null){
			if(preSql.equals(DEFAULT_PRE_SQL)){
				writeTemp = writeTemp.replace("\"preSql\": []", String.format("\"preSql\": [\"%s\"]", String.format(DEFAULT_PRE_SQL, tableName)));
			}else{
				writeTemp = writeTemp.replace("\"preSql\": []", String.format("\"preSql\": [\"%s\"]", preSql));
			}
		}
		writeTemp = writeTemp.replace("\"writeMode\": \"\"", String.format("\"writeMode\": \"%s\"", "insert"));
		return writeTemp;
	}

	private String getReader(String tableName,String[] columns,String where,String querySql) throws IOException {
		String readerTemp = FileUtils.readFileToString(new File(String.format("%s//%s//plugin_job_template.json",
				CoreConstant.DATAX_PLUGIN_READER_HOME,from_db)));
		if(where!=null){
			if(readerTemp.indexOf("\"where\": \"\"")>-1){
				readerTemp = readerTemp.replace("\"where\": \"\"", String.format("\"where\": \"%s\"", where));
			}else{
				readerTemp = readerTemp.replace("\"username\": \"\"", String.format("\"where\": \"%s\",\"username\": \"\"", where));
			}
		}
		readerTemp = readerTemp.replace("\"username\": \"\"", String.format("\"username\": \"%s\"", from_userName));
		readerTemp = readerTemp.replace("\"password\": \"\"", String.format("\"password\": \"%s\"", from_pwd));
		if(columns!=null){
			readerTemp = readerTemp.replace("\"column\": []", String.format("\"column\": [%s]", getColumns(columns)));
		}else{
			readerTemp = readerTemp.replace("\"column\": []", "\"column\": [\"*\"]");
		}
		
		if(querySql!=null){
			if(readerTemp.indexOf("\"querySql\": []")>-1){
				readerTemp = readerTemp.replace("\"querySql\": []", String.format("\"querySql\": [%s]", querySql));
			}else{
				readerTemp = readerTemp.replace("\"table\": []", String.format("\"querySql\": [%s],\"table\": []", querySql));
			}
		}
		readerTemp = readerTemp.replace("\"table\": []", String.format("\"table\": [\"%s\"]", tableName));
		readerTemp = readerTemp.replace("\"jdbcUrl\": []", String.format("\"jdbcUrl\": [\"%s\"]", from_jdbcUrl));
		return readerTemp;
	}
}
