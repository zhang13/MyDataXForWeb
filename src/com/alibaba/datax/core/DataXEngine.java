package com.alibaba.datax.core;

import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.ColumnCast;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.JobContainer;
import com.alibaba.datax.core.job.meta.ExecuteMode;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.ConfigurationValidate;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
/**
 * 
 * @author 张超

 * 2016年2月24日
 */
public class DataXEngine {
	private final Logger LOG = LoggerFactory.getLogger(DataXEngine.class);
	private String RUNTIME_MODE;
	private boolean isJobPath=false;//是job文件路径还是json字符串
	
	public DataXEngine() {
		super();
	}
	
    public DataXEngine(boolean isJobPath) {
		super();
		this.isJobPath = isJobPath;
	}

	/* check job model (job/task) first */
    public void start(String jobPathOrJobJson) throws Throwable {

    	Configuration allConf = entry(jobPathOrJobJson);
        // 绑定column转换信息
        ColumnCast.bind(allConf);

        /**
         * 初始化PluginLoader，可以获取各种插件配置
         */
        LoadUtil.bind(allConf);

        boolean isJob = !("taskGroup".equalsIgnoreCase(allConf
                .getString(CoreConstant.DATAX_CORE_CONTAINER_MODEL)));

        AbstractContainer container;
        long instanceId;
        int taskGroupId = -1;
        if (isJob) {
            allConf.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_MODE, RUNTIME_MODE);
            container = new JobContainer(allConf);
            instanceId = allConf.getLong(
                    CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, 0);

        } else {
            container = new TaskGroupContainer(allConf);
            instanceId = allConf.getLong(
                    CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
            taskGroupId = allConf.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
        }

        //缺省打开perfTrace
        boolean traceEnable = allConf.getBool(CoreConstant.DATAX_CORE_CONTAINER_TRACE_ENABLE, true);
        boolean perfReportEnable = allConf.getBool(CoreConstant.DATAX_CORE_REPORT_DATAX_PERFLOG, true);

        int priority = 0;
        try {
            priority = Integer.parseInt(System.getenv("SKYNET_PRIORITY"));
        }catch (NumberFormatException e){
            LOG.warn("prioriy set to 0, because NumberFormatException, the value is: "+System.getProperty("PROIORY"));
        }

        Configuration jobInfoConfig = allConf.getConfiguration(CoreConstant.DATAX_JOB_JOBINFO);
        //初始化PerfTrace
        PerfTrace perfTrace = PerfTrace.getInstance(isJob, instanceId, taskGroupId, priority, traceEnable);
        perfTrace.setJobInfo(jobInfoConfig);
        perfTrace.setPerfReportEnalbe(perfReportEnable);
        container.start();
    }


    // 注意屏蔽敏感信息
    public String filterJobConfiguration(final Configuration configuration) {
        Configuration jobConfWithSetting = configuration.getConfiguration("job").clone();

        Configuration jobContent = jobConfWithSetting.getConfiguration("content");

        filterSensitiveConfiguration(jobContent);

        jobConfWithSetting.set("content",jobContent);

        return jobConfWithSetting.beautify();
    }

    public Configuration filterSensitiveConfiguration(Configuration configuration){
        Set<String> keys = configuration.getKeys();
        for (final String key : keys) {
            boolean isSensitive = StringUtils.endsWithIgnoreCase(key, "password")
                    || StringUtils.endsWithIgnoreCase(key, "accessKey");
            if (isSensitive && configuration.get(key) instanceof String) {
                configuration.set(key, configuration.getString(key).replaceAll(".", "*"));
            }
        }
        return configuration;
    }

    long jobId=-1;// 如果用户没有明确指定jobid, 则 datax.py 会指定 jobid 默认值为-1
    private Configuration entry(String jobPathOrJobJson) throws Throwable {
        
        Configuration configuration = null;
        if(isJobPath){
        	configuration = ConfigParser.parse(jobPathOrJobJson);
        }else{
        	configuration = ConfigParser.parseForJson(jobPathOrJobJson);
        }

        return entryConfiguration(configuration);
    }

	private Configuration entryConfiguration(Configuration configuration) {
		RUNTIME_MODE = ExecuteMode.STANDALONE.getValue();
        boolean isStandAloneMode = "standalone".equalsIgnoreCase(RUNTIME_MODE);
        if (!isStandAloneMode && jobId == -1) {
            // 如果不是 standalone 模式，那么 jobId 一定不能为-1
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "非 standalone 模式必须在 URL 中提供有效的 jobId.");
        }
        configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, jobId);
        //打印vmInfo
//        VMInfo vmInfo = VMInfo.getVmInfo();
//        if (vmInfo != null) {
//            LOG.info(vmInfo.toString());
//        }
//
//        LOG.info("\n" + Engine.filterJobConfiguration(configuration) + "\n");

        LOG.debug(configuration.toJSON());
        ConfigurationValidate.doValidate(configuration);
		return configuration;
	}

    /**
     * -1 表示未能解析到 jobId
     *
     *  only for dsc & ds & datax 3 update
     */
    private long parseJobIdFromUrl(List<String> patternStringList, String url) {
        long result = -1;
        for (String patternString : patternStringList) {
            result = doParseJobIdFromUrl(patternString, url);
            if (result != -1) {
                return result;
            }
        }
        return result;
    }

    private long doParseJobIdFromUrl(String patternString, String url) {
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(url);
        if (matcher.find()) {
            return Long.parseLong(matcher.group(1));
        }

        return -1;
    }
}
