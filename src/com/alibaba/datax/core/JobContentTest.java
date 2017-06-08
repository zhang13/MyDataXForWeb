package com.alibaba.datax.core;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.util.ExceptionTracker;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.JobContentUtil;

public class JobContentTest {

	public static void main(String[] args) {
		try {
			new DataXTableTransfer().startAll(new SimpleTableTrasferConverter() {
				
				@Override
				public void createrTable(String tableName) {
					
				}

				@Override
				public String getWritePreSql() {
					return JobContentUtil.DEFAULT_PRE_SQL;
				}

				@Override
				public void doBefore(String tableName) {
					// TODO Auto-generated method stub
					
				}

				@Override
				public void doEnd(String tableName) {
					// TODO Auto-generated method stub
					
				}
			});
		} catch (Exception e) {
			throw DataXException.asDataXException(
					FrameworkErrorCode.CONFIG_ERROR, "获取作业配置信息失败:", e);
		} catch (Throwable e) {
			System.out.println("\n\n经DataX智能分析,该任务最可能的错误原因是:\n"
					+ ExceptionTracker.trace(e));
		}
	}
}
