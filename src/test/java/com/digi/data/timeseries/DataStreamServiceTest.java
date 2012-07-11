package com.digi.data.timeseries;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStreamServiceTest {
	private static final Logger log = LoggerFactory
			.getLogger(DataStreamServiceTest.class);

	public static final DataStreamService service = 
			DataStreamService.getServiceForHost("", "", "");
	
	@Test
	public void testService() throws Exception {
		DataStream<Double> stream = service.getStream("stream", Double.class);
		stream.refresh();
		for(DataPoint<Double> d : stream.get(0, System.currentTimeMillis())) {
			System.err.println(d.getValue());
		}
		Assert.assertEquals(Integer.class, service.getStream("stream").getValueClass());
		Assert.assertEquals("test", service.getStream("stream").getDescription());
		Assert.assertEquals("units", service.getStream("stream").getUnits());
	}
}
