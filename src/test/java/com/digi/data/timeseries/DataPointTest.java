package com.digi.data.timeseries;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataPointTest {
	private static final Logger log = LoggerFactory.getLogger(DataPointTest.class);
	
	@Test
	public void testGetValue() {
		Assert.assertEquals("test", new DataPoint<String>(String.class, "test").getValue());
		Assert.assertEquals(Integer.valueOf(1), new DataPoint<Integer>(Integer.class, "1").getValue());
		Assert.assertEquals(Float.valueOf(1.0f), new DataPoint<Float>(Float.class, "1.0").getValue());
	}
}
