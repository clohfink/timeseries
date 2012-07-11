package com.digi.data.timeseries;

import java.io.StringWriter;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.util.Base64;
/**
 * Service for retrieving DataStream objects which can be used to query data over ranges
 * or aggregates.
 */
public class DataStreamService {
	private static final Logger log = LoggerFactory.getLogger(DataStreamService.class);
	static final AsyncHttpClient httpClient = new AsyncHttpClient();
	private String username;
	private String password;

	private String host;
	private String auth;
	
	private DataStreamService(String username, String password, String host) {
		super();
		this.username = username;
		this.password = password;
		this.host = host;
		String userpassword = username + ":" + password;
		this.auth = Base64.encode(userpassword.getBytes()).trim(); 
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
	 * this stream will be represented as whatever class represents the dataType set in
	 * the streams meta data. To represent them as something else see
	 * getStream(String streamName, Class type)
	 * 
	 * @param streamName
	 * @return DataStream
	 */
	public DataStream<?> getStream(String streamName) {
		return new DataStream(streamName, this);
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
		return new DataStream<DataType>(streamName, type, this);
	}
	
	String getAuthHeader() {
		return auth;
	} 

	String getHost() {
		return host;
	}
	
	/*
	 * source: http://stackoverflow.com/questions/4412848/xml-node-to-string-in-java
	 */
	static String nodeToString(Node node) {
		StringWriter sw = new StringWriter();
		try {
			Transformer t = TransformerFactory.newInstance().newTransformer();
			t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
			t.setOutputProperty(OutputKeys.INDENT, "yes");
			t.transform(new DOMSource(node), new StreamResult(sw));
		} catch (TransformerException te) {
			log.error(te.getMessage(), te);
		}
		return sw.toString();
	}
}
