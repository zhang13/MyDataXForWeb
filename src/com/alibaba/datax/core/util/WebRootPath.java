package com.alibaba.datax.core.util;

/**
 * 
 * @author 张超

 * 2016年2月24日
 */
public class WebRootPath {
	/**
	 * 获取应用根路径,不带后面的/
	 * @return
	 */
	public String getWebRootPath(){
		String path = getClass().getProtectionDomain().getCodeSource()
		.getLocation().getPath();
		if (path.indexOf("WEB-INF") > 0) {
			path = path.substring(0, path.indexOf("/WEB-INF/") );
		}
		/*
		 * 在覆盖率测试时，上面得到的结果可能是：D:\workspace\.metadata\.plugins\com.mountainminds.eclemma.core\.instr\ec4fcf671458d86af213fda104851119
		 * 这时我们可以直接用开发人员统一用的路径。
		 */
		if(path.contains(".metadata")){
			path="D:/workspace/SubCenterMonitor/WebRoot";
		}
		return path;
	}
}
