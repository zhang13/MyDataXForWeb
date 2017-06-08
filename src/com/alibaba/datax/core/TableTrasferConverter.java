package com.alibaba.datax.core;

public interface TableTrasferConverter {
	public String convertTableName(String tableName);
	public void createrTable(String tableName);
	public String getWritePreSql();
	public void doBefore();
	public void doEnd();
}
