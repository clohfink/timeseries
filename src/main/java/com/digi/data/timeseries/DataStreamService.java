package com.digi.data.timeseries;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Service for retrieving DataStream objects which can be used to query data over ranges
 * or aggregates.
 */
public class DataStreamService {
	private static final Logger log = LoggerFactory.getLogger(DataStreamService.class);
	private String username;
	private String password;
	private String host;
	
	private DataStreamService(String username, String password, String host) {
		super();
		this.username = username;
		this.password = password;
		this.host = host;
	}
	
	/**
	 * Factory method for retrieving a DataStreamService. The username and
	 * password will be used in all sequential queries related to DataStreams
	 * created by this service.
	 * 
	 * If using Spring this is a good place to inject a bean for autowiring
	 * <pre>
	 * 	&lt;bean id="dataStreamService" class="com.digi.data.timeseries.DataStreamService"
	 * 				factory-method="getService"> 
	 * 		&lt;constructor-arg ref="${username}" />
	 * 		&lt;constructor-arg ref="${password}" />
	 *  &lt;/bean>
	 * </pre>
	 * 
	 * @param host
	 * @param username
	 * @param password
	 * @return
	 */
	public static DataStreamService getService(String username, String password) {
		return new DataStreamService(username, password, "my.idigi.com");
	}

	/**
	 * @see getService
	 */
	public static DataStreamService getServiceForHost(String host, String username, String password) {
		return new DataStreamService(username, password, host);
	}
	
	/**
	 * get a datastream that represents a given name.  Data point values retrieved from
	 * this stream will be represented as Doubles, to represent them as something else see
	 * getStream(String streamName, Class type) 
	 * 
	 * @param streamName
	 * @return DataStream
	 */
	public DataStream<Double> getStream(String streamName) {
		return new DataStream<Double>(streamName, Double.class, username, password, host);
	}
	
	/**
	 * Get stream of a given name, all Data Point values will be converted to the type provided
	 * when calling "getValue()".  The type must have a constructor that takes a String argument to
	 * convert from the XML provided from the /ws/DataPoint HTTP call
	 * 
	 * @param streamName
	 * @param type
	 * @return
	 */
	public <DataType> DataStream<DataType> getStream(String streamName, Class<? extends DataType> type) {
		return new DataStream<DataType>(streamName, type, username, password, host);
	}
}
