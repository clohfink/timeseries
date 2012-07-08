package com.digi.data.timeseries;

import java.util.Date;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStreamServiceTest {
	private static final Logger log = LoggerFactory
			.getLogger(DataStreamServiceTest.class);
	

	public static final DataStreamService service = 
			DataStreamService.getService("username", "password");
	
	@Test
	public void testService() {
		// TODO
	}
}
