package com.digi.data.timeseries;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import javax.naming.AuthenticationException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.ListenableFuture;
import com.ning.http.client.Response;
import com.ning.http.util.Base64;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;

public class StreamSnapshot<DataType> implements Iterator<DataPoint<DataType>>, Iterable<DataPoint<DataType>> {
	private static final Logger log = LoggerFactory.getLogger(StreamSnapshot.class);
	private static final AsyncHttpClient httpClient = new AsyncHttpClient();
	static final XStream xstream = new XStream(new StaxDriver());
	static {
		xstream.alias("DataPoint", DataPoint.class);
	}

	private Interval interval;
	private Aggregate aggregate;
	private DataStream<DataType> stream;
	private List<DataPoint<DataType>> buffer;
	private long start;
	private long end;
	private ListenableFuture<Response> future; 
	private String auth;
	private Document dom;

	public StreamSnapshot(DataStream<DataType> stream, long start, long end, 
			Interval interval, Aggregate aggregate) {
		this.stream = stream;
		this.start = start;
		this.end = end;
		this.interval = interval == null ? Interval.None : interval;
		this.aggregate = aggregate == null ? Aggregate.None : aggregate;
		boolean noIntervalWithAgg = interval.equals(Interval.None)
				&& !aggregate.equals(Aggregate.None);
		boolean noAggWithInterval = !interval.equals(Interval.None)
				&& aggregate.equals(Aggregate.None);
		if (noAggWithInterval || noIntervalWithAgg) {
			throw new IllegalArgumentException(
					"Aggregate and Interval must both be specified");
		}
		String userpassword = stream.getUser() + ":" + stream.getPassword();
		this.auth = Base64.encode(userpassword.getBytes()).trim(); 
	}

	public synchronized ListenableFuture<Response> fetchNextChunk()
			throws IOException {
		if (future == null) {
			StringBuilder url = new StringBuilder("https://");
			url.append(stream.getHost()).append("/ws/DataPoint/");
			url.append(stream.getStreamName());
			url.append("?startTime=").append(start);
			url.append("&endTime=").append(end);
			if (!Interval.None.equals(interval)) {
				// we are doing a rollup
				url.append("&rollupInterval=").append(
						interval.name().toLowerCase());
				url.append("&rollupMethod=").append(
						aggregate.name().toLowerCase());
			} 
			if(dom != null) {
				NodeList nl = dom.getElementsByTagName("pageCursor");
				if(nl.getLength() > 0) {
					String cursor = nl.item(0).getTextContent();
					url.append("&pageCursor=").append(cursor);
				}
			}
			log.info("query: "+url);
			future = httpClient.prepareGet(url.toString())
					.setHeader("Content-type", "text/xml; charset=utf-8")
					.setHeader("Authorization", "Basic " + auth).execute();
		}
		return future;
	}

	/**
	 * returns true if there is additional datapoints available in this time period
	 */
	public synchronized boolean hasNext() {
		if (buffer == null || buffer.size() == 0) {
			// empty buffer, we are either done or need to fetch more
			if(buffer == null) buffer = new LinkedList();
			try {
				// wait for next chunks response
				Response rsp = fetchNextChunk().get(); 
				if(rsp.getStatusCode() == 401) {
					throw new RuntimeException("Invalid credentials, HTTP 401");
				} else if(rsp.getStatusCode() != 200) {
					log.error(rsp.getResponseBody());
					throw new RuntimeException("Unexpected status code: "+rsp.getStatusCode());
				}
				// parse xml into dom
				DocumentBuilderFactory dbf = DocumentBuilderFactory
						.newInstance();
				DocumentBuilder db = dbf.newDocumentBuilder();
				log.info(rsp.getResponseBody());
				InputSource is = new InputSource(new StringReader(rsp.getResponseBody()));
				dom = db.parse(is);
				// get all the DataPoint elements
				NodeList points = dom.getElementsByTagName("DataPoint");
				// add all the data points to the buffer
				for (int i = 0; i < points.getLength(); i++) {
					Node dataPoint = points.item(i);
					DataPoint<DataType> dp = (DataPoint<DataType>) xstream.fromXML(nodeToString(dataPoint));
					dp.setValueClass(stream.getValueClass());
					buffer.add(dp);
				}
				// we consumed this future, remove it
				future = null;
			} catch (Exception e) {
				log.error(e.getMessage(), e);
			}
		}
		return buffer.size() > 0;
	}
	
	/**
	 * fetches the next data point in chronological order
	 */
	public synchronized DataPoint<DataType> next() {
		if(!hasNext()) throw new IndexOutOfBoundsException();
		DataPoint<DataType> ret = buffer.remove(0);
		if(buffer.size() == 0) {
			// start fetching next chunk
			try {
				fetchNextChunk();
			} catch (IOException e) { 
				log.error(e.getMessage(), e);
			}
		}
		return ret;
	}

	public void remove() {
		// TODO record last read UUID and send an HTTP DELETE to support
		throw new RuntimeException("Not implemented");
	}

	/*
	 * source: http://stackoverflow.com/questions/4412848/xml-node-to-string-in-java
	 */
	private static String nodeToString(Node node) {
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

	/**
	 * This is created for convience so can be used in for loops, but to meet
	 * the iteratable contract we need to be able to iterate more then once, this
	 * is inefficient but we will just create a new stream each time this is called
	 * 
	 * TODO cache the results of the fetches so do not need to be refetched per
	 * iteration
	 */
	public Iterator<DataPoint<DataType>> iterator() {
		return new StreamSnapshot<DataType>(stream, start, end, interval, aggregate);
	}

}
