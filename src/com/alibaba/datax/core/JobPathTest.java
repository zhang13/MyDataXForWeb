package com.alibaba.datax.core;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.core.util.ExceptionTracker;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;

public class JobPathTest {
	public static void main(String[] args) throws Exception {
//		String dir = System.getProperty("user.dir");
		// String [] strs =
		// {"-mode","standalone","-jobid","-1","-job",String.format("%s//job//mysql2sqlserver.json",
		// dir)};
//		System.setProperty("datax.home", dir);
		int exitCode = 0;
		try {
			DataXEngine dataXEngine = new DataXEngine(true);
			dataXEngine.start(String.format("%s//job//accessReader.json",
					CoreConstant.DATAX_HOME));
		} catch (Throwable e) {
			exitCode = 1;
			System.out.println("\n\n经DataX智能分析,该任务最可能的错误原因是:\n"
					+ ExceptionTracker.trace(e));
			if (e instanceof DataXException) {
				DataXException tempException = (DataXException) e;
				ErrorCode errorCode = tempException.getErrorCode();
				if (errorCode instanceof FrameworkErrorCode) {
					FrameworkErrorCode tempErrorCode = (FrameworkErrorCode) errorCode;
					exitCode = tempErrorCode.toExitValue();
				}
			}
			 System.exit(exitCode);
		}
		 System.exit(exitCode);
	}
}
