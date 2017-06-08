package com.alibaba.datax.core;

import java.util.List;

import com.alibaba.datax.core.util.JobContentUtil;
import com.alibaba.datax.core.util.PropertiesReader;

/**
 * 
 * @author 张超
 * 
 *         2016年3月8日
 */
public class DataXTableTransfer {
	private PropertiesReader propertiesReader;
	public DataXTableTransfer(String transferConf) {
		propertiesReader = new PropertiesReader(transferConf);
	}

	/**
	 * 默认配置文件名称为dataTransferConf.properties
	 */
	public DataXTableTransfer() {
		propertiesReader = new PropertiesReader();
	}

	/**
	 * 表名，列名从配置文件里读取
	 * @throws Throwable
	 */
	public void startAll(SimpleTableTrasferConverter convertFunc) throws Throwable {
		List<String> tableList = propertiesReader.getConvertTables();
		if(tableList==null||tableList.size()==0){
			throw new RuntimeException(String.format("没有表需要迁移！请在配置文件%s中配置",propertiesReader.getPropertiesFileName()));
		}
		String tableName = null;
		for (int i = 0; i < tableList.size(); i++) {
			tableName = tableList.get(i);
			String[] columnArr = propertiesReader.getTableColumns(tableName);
			convertFunc.doBefore(tableName);
			DataXEngine dataXEngine = new DataXEngine();
			convertFunc.createrTable(tableName);
			dataXEngine.start(new JobContentUtil().getJobJson(tableName,columnArr,convertFunc.getWritePreSql()));
			convertFunc.doEnd(tableName);
		}
	}
	
	/**
	 * 实现DefaultTableTrasferConverter接口配置迁移参数
	 * @throws Throwable
	 */
	public void startAll(DefaultTableTrasferConverter convertFunc) throws Throwable {
		List<String> tableList = convertFunc.getConvertTables();
		if(tableList==null||tableList.size()==0){
			throw new RuntimeException("没有表需要迁移！");
		}
		String tableName = null;
		for (int i = 0; i < tableList.size(); i++) {
			String baseTableName = tableList.get(i);
			String[] columnArr = convertFunc.getTableColumns(baseTableName);
			tableName = convertFunc.convertTableName(baseTableName);
			convertFunc.doBefore(tableName);
			DataXEngine dataXEngine = new DataXEngine();
			convertFunc.createrTable(baseTableName,tableName);
			dataXEngine.start(new JobContentUtil().getJobJson(tableName,columnArr,convertFunc.getWritePreSql()));
			convertFunc.doEnd(tableName);
		}
	}
	/**
	 * 用传进来的参数，不用配置文件里的内容，但是表名、列表还是从文件里读取
	 * @param from_userName
	 * @param from_pwd
	 * @param from_jdbcUrl
	 * @param from_db
	 * @param to_userName
	 * @param to_pwd
	 * @param to_jdbcUrl
	 * @param to_db
	 * @param convertFunc
	 * @throws Throwable
	 */
	public void startAll(String from_userName, String from_pwd,
			String from_jdbcUrl, String from_db, String to_userName, String to_pwd,
			String to_jdbcUrl,String to_db,SimpleTableTrasferConverter convertFunc) throws Throwable {
		List<String> tableList = propertiesReader.getConvertTables();
		if(tableList==null||tableList.size()==0){
			throw new RuntimeException(String.format("没有表需要迁移！请在配置文件%s中配置",propertiesReader.getPropertiesFileName()));
		}
		String tableName = null;
		for (int i = 0; i < tableList.size(); i++) {
			tableName = tableList.get(i);
			String[] columnArr = propertiesReader.getTableColumns(tableName);
			convertFunc.doBefore(tableName);
			DataXEngine dataXEngine = new DataXEngine();
			convertFunc.createrTable(tableName);
			dataXEngine.start(new JobContentUtil(from_userName, from_pwd,
						from_jdbcUrl, from_db, to_userName, to_pwd,
						to_jdbcUrl,to_db).getJobJson(tableName,columnArr,convertFunc.getWritePreSql()));
			convertFunc.doEnd(tableName);
		}
	}
	
	/**
	 * 用传进来的参数，不用配置文件里的内容.实现DefaultTableTrasferConverter接口
	 * @param from_userName
	 * @param from_pwd
	 * @param from_jdbcUrl
	 * @param from_db
	 * @param to_userName
	 * @param to_pwd
	 * @param to_jdbcUrl
	 * @param to_db
	 * @throws Throwable
	 */
	public void startAll(String from_userName, String from_pwd,
			String from_jdbcUrl, String from_db, String to_userName, String to_pwd,
			String to_jdbcUrl,String to_db,DefaultTableTrasferConverter convertFunc) throws Throwable {
		List<String> tableList = convertFunc.getConvertTables();
		if(tableList==null||tableList.size()==0){
			throw new RuntimeException("没有表需要迁移！");
		}
		List<String> destTableLis = convertFunc.getDestTables();
		String fromTableName = null;
		String toTableName = null;
		for (int i = 0; i < tableList.size(); i++) {
			String baseTableName = tableList.get(i);
			fromTableName = convertFunc.convertTableName(baseTableName);
			if(destTableLis!=null&&destTableLis.size()==tableList.size()){
				toTableName = destTableLis.get(i);
			}else{
				toTableName = convertFunc.changeToTableName(fromTableName);
			}
			convertFunc.doBefore(fromTableName);
			DataXEngine dataXEngine = new DataXEngine();
			convertFunc.createrTable(baseTableName,toTableName);
			dataXEngine.start(new JobContentUtil(from_userName, from_pwd,
						from_jdbcUrl, from_db, to_userName, to_pwd,
						to_jdbcUrl,to_db).getJobJson(fromTableName, toTableName, convertFunc.getTableColumns(baseTableName), convertFunc.getWriteColumns(baseTableName), convertFunc.getWhere(), convertFunc.getQuerySql(), convertFunc.getWritePreSql()));
			convertFunc.doEnd(fromTableName);
		}
	}
}
