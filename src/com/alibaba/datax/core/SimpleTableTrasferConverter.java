package com.alibaba.datax.core;


/**
 * 
 * @author 张超

 * 2016年3月8日
 */
public interface SimpleTableTrasferConverter {
	/**
	 * 建表
	 * @param tableName
	 */
	public void createrTable(String tableName);
	/**
	 * 写入前执行的sql
	 * @return
	 */
	public String getWritePreSql();
	/**
	 * 开始迁移一个表前的操作
	 */
	public void doBefore(String tableName);
	/**
	 * 完成一个表迁移后的操作
	 */
	public void doEnd(String tableName);
}
