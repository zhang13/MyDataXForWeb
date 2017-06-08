package com.alibaba.datax.core.util;

import java.io.FileInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
/**
 * 从dataTransferConf.properties文件中读取设置
 * @author 张超
 *
 */
public class PropertiesReader {
	private String propertiesFileName;
	
	/**
	 * 默认配置文件名称为dataTransferConf.properties
	 */
	public PropertiesReader() {
		propertiesFileName = CoreConstant.DATAX_CONF_CONVERT_PATH;
	}

	public PropertiesReader(String propertiesFileName) {
		this.propertiesFileName = propertiesFileName;
	}
	
	public String getPropertiesFileName() {
		return propertiesFileName;
	}

	public String getFromDB(){
		return getPropValueByName("from")+"reader";
	}
	
	public String getToDB(){
		return getPropValueByName("to")+"writer";
	}
	
	public String getFromDBUserName(){
		return getPropValueByName("from_username");
	}
	
	public String getFromDBPwd(){
		return getPropValueByName("from_password");
	}
	
	public String getFromDBJdbcUrl(){
		return getPropValueByName("from_jdbcUrl");
	}
	
	public String getToDBUserName(){
		return getPropValueByName("to_username");
	}
	
	public String getToDBPwd(){
		return getPropValueByName("to_password");
	}
	
	public String getToDBJdbcUrl(){
		return getPropValueByName("to_jdbcUrl");
	}
	
	public List<String> getConvertTables(){
		String tableStr = getPropValueByName("convertTables");
		String[] tableArr = tableStr.split(",");
		return Arrays.asList(tableArr);
	}
	
	private String getPropValueByName(String propName){
		Properties prop = getProps();
		String propValue=(String)prop.get(propName);
		if(propValue==null){
			throw new RuntimeException("没有在"+propertiesFileName+"中配置"+propName+"参数");
		}
		
		return propValue;
	}
	
	public String[] getTableColumns(String tableName){
		String convertColumnsJson = getPropValueByName("convertColumns");
		if(convertColumnsJson==null||convertColumnsJson.isEmpty()){
			return null;
		}
		JSONObject columnsMap = JSON.parseObject(convertColumnsJson);
		JSONArray columnsArray = columnsMap.getJSONArray(tableName);
		if(columnsArray==null){
			return null;
		}
		return columnsArray.toArray(new String[]{});
	}

	private Properties prop;
	private Properties getProps() {
		try {
			if(prop==null){
				prop=new Properties();
				prop.load(new FileInputStream(propertiesFileName));
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		return prop;
	}
	
	public String getStringProp(String propName){
		Properties prop = getProps();
		String propValue=(String)prop.get(propName);
		return propValue;
	}
}
