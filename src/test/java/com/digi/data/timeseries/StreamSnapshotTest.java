package com.digi.data.timeseries;

import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

public class StreamSnapshotTest {

	private static String DATA_POINT = 
			"<DataPoint><cstId>2</cstId><streamId>device1/temp</streamId>"+
			"<timestamp>1341460800000</timestamp><data>1.0</data></DataPoint>";
	
	@Test
	public void testXstreamConvert() {
		DataPoint<Float> dp = (DataPoint<Float>) StreamSnapshot.xstream.fromXML(DATA_POINT);
		Assert.assertEquals(1341460800000L, dp.getTimestamp());
		Assert.assertEquals("device1/temp", dp.getStreamId());
		dp.setValueClass(Float.class);
		Assert.assertTrue(1.0f == dp.getValue());
	}
	
	@Test
	public void testFetchNext() throws Exception {
		DataStream<Float> stream = new DataStream<Float>("device1/temp",
				Float.class, "", "");
		
		for (DataPoint<Float> data : stream.get(Aggregate.Sum, Interval.Hour, 0, System.currentTimeMillis())) {
			float sumForHour = data.getValue();
			Date hour = new Date(data.getTimestamp());
			System.err.println("Sum for the hour starting at " + hour + " is " + sumForHour);
		}
	}
}
