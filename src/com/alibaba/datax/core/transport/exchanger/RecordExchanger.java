/**
 *  (C) 2010-2014 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.datax.core.transport.exchanger;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.record.TerminateRecord;

public class RecordExchanger implements RecordSender, RecordReceiver {

	private Channel channel;

	private Configuration configuration;

	private static Class<? extends Record> RECORD_CLASS;

	private volatile boolean shutdown = false;

	@SuppressWarnings("unchecked")
	public RecordExchanger(final Channel channel) {
		assert channel != null;
		this.channel = channel;
		this.configuration = channel.getConfiguration();
		try {
			RecordExchanger.RECORD_CLASS = (Class<? extends Record>) Class
					.forName(configuration.getString(
                            CoreConstant.DATAX_CORE_TRANSPORT_RECORD_CLASS,
                            "com.alibaba.datax.core.transport.record.DefaultRecord"));
		} catch (ClassNotFoundException e) {
			throw DataXException.asDataXException(
					FrameworkErrorCode.CONFIG_ERROR, e);
		}
	}

	@Override
	public Record getFromReader() {
		if(shutdown){
			throw DataXException.asDataXException(CommonErrorCode.SHUT_DOWN_TASK, "");
		}
		Record record = this.channel.pull();
		return (record instanceof TerminateRecord ? null : record);
	}

	@Override
	public Record createRecord() {
		try {
			return RECORD_CLASS.newInstance();
		} catch (Exception e) {
			throw DataXException.asDataXException(
					FrameworkErrorCode.CONFIG_ERROR, e);
		}
	}

	@Override
	public void sendToWriter(Record record) {
		if(shutdown){
			throw DataXException.asDataXException(CommonErrorCode.SHUT_DOWN_TASK, "");
		}
		this.channel.push(record);
	}

	@Override
	public void flush() {
	}

	@Override
	public void terminate() {
		if(shutdown){
			throw DataXException.asDataXException(CommonErrorCode.SHUT_DOWN_TASK, "");
		}
		this.channel.pushTerminate(TerminateRecord.get());
	}

	@Override
	public void shutdown(){
		shutdown = true;
	}
}
