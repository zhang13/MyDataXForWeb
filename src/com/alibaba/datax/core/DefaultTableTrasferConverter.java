package com.alibaba.datax.core;

import java.util.List;

import com.alibaba.datax.core.util.JobContentUtil;

public abstract class DefaultTableTrasferConverter{
	/**
	 * 获取表要提取的列明
	 * @param tableName
	 * @return
	 */
	public abstract String[] getTableColumns(String tableName);
	/**
	 * 获取歉意表名列表
	 * @return
	 */
	public abstract List<String> getConvertTables();
	
	/**
	 * 获取目标表名列表,返回null则取与源表一样表明
	 * @return
	 */
	public abstract List<String> getDestTables();
	
	/**
	 * 转换表名称
	 * @param baseTableName
	 * @return 真实表名
	 */
	public abstract String convertTableName(String baseTableName);
	
	/**
	 * 更改to表名称
	 * @param tableName
	 * @return 真实表名
	 */
	public abstract String changeToTableName(String tableName);
	
	/**
	 * 建表
	 * @param baseTableName
	 * @param tableName
	 */
	public abstract void createrTable(String baseTableName,String tableName);
	/**
	 * 写入前执行的sql
	 * @return
	 */
	public String getWritePreSql(){
		return JobContentUtil.DEFAULT_PRE_SQL;
	}
	
	/**
	 * 开始迁移一个表前的操作
	 */
	public void doBefore(String tableName){
		
	}
	/**
	 * 完成一个表迁移后的操作
	 */
	public void doEnd(String tableName){
		
	}
	
	/**
	 * 条件语句
	 * @return
	 */
	public String getWhere(){
		return null;
	}
	
	/**
	 * 查询语句
	 * 当用户配置querySql时，MysqlReader直接忽略table、column、where条件的配置，querySql优先级大于table、column、where选项。
	 * @return
	 */
	public String getQuerySql(){
		return null;
	}
	
	/**
	 * 写入字段
	 * @return
	 */
	public  String[] getWriteColumns(String tableName){
		return getTableColumns(tableName);
	}
}
