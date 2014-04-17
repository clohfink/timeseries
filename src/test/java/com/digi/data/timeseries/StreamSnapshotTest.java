package com.digi.data.timeseries;

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
	
	/**
	 * Ensure that if an unexpected field is encountered when parsing a 
	 * DataPoint that an exception is not raised.
	 */
	@Test
	public void testXstreamConversionUnknownElement() {
		String unknownElement = "<DataPoint>" +
			"<cstId>2</cstId>" +
			"<streamId>device1</streamId>" +
			"<newExcitingField>so exciting</newExcitingField>" +
			"<timestamp>1341460800000</timestamp>" +
			"<data>1.0</data>" +
			"</DataPoint>";
		StreamSnapshot.xstream.fromXML(unknownElement);
	}
	
	@Test
	public void testFetchNext() throws Exception {
		// todo
	}
}
