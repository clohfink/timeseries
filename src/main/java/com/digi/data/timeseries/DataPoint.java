package com.digi.data.timeseries;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.UUID;

public class DataPoint<Type> { 
	/**
	 * UUID representing this data point
	 */
	private UUID id = null;
	
	/**
	 * Timestamp reported by client
	 */
	private long timestamp=0;
	
	/**
	 * Name of the stream this datapoint was uploaded to
	 */
	private String streamId;
	
	/**
	 * Id for customer who owns the stream
	 */
	private int cstId;
	
	/**
	 * Time this data point was uploaded to the server
	 */
	private long serverTimestamp=0;
	
	/**
	 * description of data point (optional)
	 */
	private String description = "";
	
	/**
	 * quality of data point (optional)
	 */
	private int quality = 0;
	
	/**
	 * Location of data point.  
	 */
	private String location;
	
	/**
	 * value of the data
	 */
	private String data;
	
	/**
	 * Class of the type to convert the data to
	 */
	private Class<? extends Type> valueClass;
	
	public DataPoint() {}
	
	// visable for testing
	DataPoint(Class<? extends Type> valueClass, String data) {
		this.valueClass = valueClass;
		this.data = data;
	}
	
	/**
	 * retrieves the value of this data points value in its generic type
	 * @return
	 */
	public Type getValue() {
		Type ret = null;
        try {
    		Constructor constructor = valueClass.getConstructor(new Class[]{String.class});
			ret = (Type) constructor.newInstance(data);
		} catch (Exception e) {
			throw new ClassCastException();
		} 
        return ret;
	}

	/**
	 * UUID representing this data point
	 */
	public UUID getId() {
		return id;
	}


	/**
	 * UUID representing this data point
	 */
	public void setId(UUID id) {
		this.id = id;
	}

	/**
	 * Timestamp reported by client
	 */
	public long getTimestamp() {
		return timestamp;
	} 

	/**
	 * Time this data point was uploaded to the server
	 */
	public long getServerTimestamp() {
		return serverTimestamp;
	} 

	/**
	 * description of data point (optional)
	 */
	public String getDescription() {
		return description;
	} 

	/**
	 * quality of data point (optional)
	 */
	public int getQuality() {
		return quality;
	} 

	/**
	 * Location of data point.  
	 */
	public String getLocation() {
		return location;
	} 
	
	/**
	 * Class of the type to convert the data to
	 */
	public Class<? extends Type> getValueClass() {
		return valueClass;
	}
	
	/**
	 * Class of the type to convert the data to
	 */
	public void setValueClass(Class<? extends Type> valueClass) {
		this.valueClass = valueClass;
	}

	/**
	 * Name of the stream this datapoint was uploaded to
	 */
	public String getStreamId() {
		return streamId;
	}

	/**
	 * Name of the stream this datapoint was uploaded to
	 */
	public void setStreamId(String streamId) {
		this.streamId = streamId;
	} 

	/**
	 * value of the data
	 */
	public String getData() {
		return data;
	}

	/**
	 * value of the data
	 */
	public void setData(String data) {
		this.data = data;
	}
}
