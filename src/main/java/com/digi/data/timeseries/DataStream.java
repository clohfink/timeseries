package com.digi.data.timeseries;

/**
 * Representation of a DataStream. Data streams represent time series data, a
 * sequence of data points. You can create a data stream with web services, by
 * uploading data points, or using the Dia or Smart energy frameworks with
 * iDigi. You can query the data by time ranges, rollup intervals and perform
 * basic aggregates.
 * 
 * @param <DataType>
 *            Type of object to convert the value to: Float, Integer, Double,
 *            String, etc...
 */
public class DataStream<DataType> {
	/**
	 * Name of the stream
	 */
	private String streamName;
	
	/**
	 * host of the iDigi server
	 */
	private String host;
	
	/**
	 * iDigi username
	 */
	private String user;
	
	/**
	 * iDigi password
	 */
	private String password;
	
	/**
	 * class of the generic type of this stream
	 */
	private Class<? extends DataType> valueClass;
	
	DataStream(String streamName, Class<? extends DataType> clazz, 
			String user, String password) {
		this(streamName, clazz, user, password, "test.idigi.com");
	}
	
	DataStream(String streamName, Class<? extends DataType> valueClass, 
			String user, String password, String host) {
		this.streamName = streamName;
		this.user = user;
		this.password = password;
		this.host = host;
		this.valueClass = valueClass;
	}

	/**
	 * Fetch the data points between two times. The returned snapshot can be
	 * used as iterator or iterable in loop, ie
	 * 
	 * <pre>
	 * DataStream&lt;Float> steram = ...
	 * for(DataPoint&lt;Float> data : stream.get(0, System.currentTimeMillis()) ){
	 * 	   float dataPointValue = data.getValue();
	 *     System.err.println(dataPointValue);
	 * }
	 * </pre>
	 * 
	 * @param start
	 *            number of ms since epoc
	 * @param end
	 *            number of ms since epoc
	 * @return
	 */
	public StreamSnapshot<DataType> get(long start, long end) {
		return new StreamSnapshot<DataType>(this, start, end, 
				Interval.None, Aggregate.None);
	}
	
	/**
	 * Fetch the aggregate for a given interval between two times. The returned
	 * snapshot can be used as iterator or iterable in loop, ie
	 * 
	 * <pre>
	 * DataStream&lt;Float> steram = ...
	 * long now = System.currentTimeMillis();
	 * for(DataPoint&lt;Float> data : stream.get(Aggregate.Sum, Interval.Hour, 0L, now)) {
	 * 	   float sumForHour = data.getValue();
	 *     Data hour = new Date(data.getTimestamp());
	 *     System.err.println("Sum for the hour starting at " + hour +" is " + sumForHour);
	 * }
	 * </pre>
	 * 
	 * @param aggregate
	 *            an algorithm to apply to the data within each interval
	 * @param interval
	 *            time frame to group data
	 * @param start
	 *            number of ms since epoc
	 * @param end
	 *            number of ms since epoc
	 * @return
	 */
	public StreamSnapshot<DataType> get(Aggregate aggregate, Interval interval,
			long start, long end) {
		return new StreamSnapshot<DataType>(this, start, end, interval, aggregate);
	}
	
	/**
	 * Fetch the sum of the values for a given interval between two times. The returned
	 * snapshot can be used as iterator or iterable in loop, ie
	 * 
	 * <pre>
	 * DataStream&lt;Float> steram = ...
	 * long now = System.currentTimeMillis();
	 * for(DataPoint&lt;Float> data : stream.getSums(Interval.Hour, 0L, now)) {
	 *     float sumForHour = data.getValue();
	 *     Data hour = new Date(data.getTimestamp());
	 *     System.err.println("Sum for the hour starting at " + hour +" is " + sumForHour);
	 * }
	 * </pre>
	 * 
	 * @param interval
	 *            time frame to group data
	 * @param start
	 *            number of ms since epoc
	 * @param end
	 *            number of ms since epoc
	 * @return
	 */
	public StreamSnapshot<DataType> getSums(Interval interval, long start, long end) {
		return get(Aggregate.Sum, interval, start, end);
	}
	
	/**
	 * Fetch the count of the values for a given interval between two times. The returned
	 * snapshot can be used as iterator or iterable in loop, ie
	 * 
	 * <pre>
	 * DataStream&lt;Float> steram = ...
	 * long now = System.currentTimeMillis();
	 * for(DataPoint&lt;Float> data : stream.getCounts(Interval.Hour, 0L, now)) {
	 *     float count = data.getValue();
	 *     Data hour = new Date(data.getTimestamp());
	 *     System.err.println("Number of data points for the hour starting at " + hour +" is " + count);
	 * }
	 * </pre>
	 * 
	 * @param interval
	 *            time frame to group data
	 * @param start
	 *            number of ms since epoc
	 * @param end
	 *            number of ms since epoc
	 * @return
	 */
	public StreamSnapshot<DataType> getCounts(Interval interval, long start, long end) {
		return get(Aggregate.Count, interval, start, end);
	}

	/**
	 * Fetch the largest value for a given interval between two times. The returned
	 * snapshot can be used as iterator or iterable in loop, ie
	 * 
	 * <pre>
	 * DataStream&lt;Float> steram = ...
	 * long now = System.currentTimeMillis();
	 * for(DataPoint&lt;Float> data : stream.getMaximums(Interval.Hour, 0L, now)) {
	 *     float max = data.getValue();
	 *     Data hour = new Date(data.getTimestamp());
	 *     System.err.println("Largest value for the hour starting at " + hour +" is " + max);
	 * }
	 * </pre>
	 * 
	 * @param interval
	 *            time frame to group data
	 * @param start
	 *            number of ms since epoc
	 * @param end
	 *            number of ms since epoc
	 * @return
	 */
	public StreamSnapshot<DataType> getMaximums(Interval interval, long start, long end) {
		return get(Aggregate.Max, interval, start, end);
	}
	
	/**
	 * Fetch the smallest value for a given interval between two times. The returned
	 * snapshot can be used as iterator or iterable in loop, ie
	 * 
	 * <pre>
	 * DataStream&lt;Float> steram = ...
	 * long now = System.currentTimeMillis();
	 * for(DataPoint&lt;Float> data : stream.getMinimums(Interval.Hour, 0L, now)) {
	 *     float min = data.getValue();
	 *     Data hour = new Date(data.getTimestamp());
	 *     System.err.println("Smallest value for the hour starting at " + hour +" is " + min);
	 * }
	 * </pre>
	 * 
	 * @param interval
	 *            time frame to group data
	 * @param start
	 *            number of ms since epoc
	 * @param end
	 *            number of ms since epoc
	 * @return
	 */
	public StreamSnapshot<DataType> getMinimums(Interval interval, long start, long end) {
		return get(Aggregate.Min, interval, start, end);
	}
	
	/**
	 * Fetch the average value for a given interval between two times. The returned
	 * snapshot can be used as iterator or iterable in loop, ie
	 * 
	 * <pre>
	 * DataStream&lt;Float> steram = ...
	 * long now = System.currentTimeMillis();
	 * for(DataPoint&lt;Float> data : stream.getAverages(Interval.Hour, 0L, now)) {
	 *     float avg = data.getValue();
	 *     Data hour = new Date(data.getTimestamp());
	 *     System.err.println("Average value for the hour starting at " + hour +" is " + avg);
	 * }
	 * </pre>
	 * 
	 * @param interval
	 *            time frame to group data
	 * @param start
	 *            number of ms since epoc
	 * @param end
	 *            number of ms since epoc
	 * @return
	 */
	public StreamSnapshot<DataType> getAverages(Interval interval, long start, long end) {
		return get(Aggregate.Average, interval, start, end);
	}

	/**
	 * Name of the stream
	 */
	public String getStreamName() {
		return streamName;
	}

	/**
	 * host of the iDigi server
	 */
	public String getHost() {
		return host;
	}

	/**
	 * iDigi username
	 */
	public String getUser() {
		return user;
	}

	/**
	 * iDigi password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * class of the generic type of this stream
	 */
	public Class<? extends DataType> getValueClass() {
		return valueClass;
	}
 
}
