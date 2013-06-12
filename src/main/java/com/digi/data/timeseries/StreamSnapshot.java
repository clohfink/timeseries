package com.digi.data.timeseries;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;

public class StreamSnapshot<DataType> implements Iterator<DataPoint<DataType>>, Iterable<DataPoint<DataType>> {
    private static final Logger log = LoggerFactory.getLogger(StreamSnapshot.class);
    static final XStream xstream = new XStream(new StaxDriver());
    static {
        xstream.alias("DataPoint", DataPoint.class);
    }

    private Interval interval;
    private Aggregate aggregate;
    private DataStream<DataType> stream;
    private DataStreamService service;
    private List<DataPoint<DataType>> buffer;
    private long start;
    private long end; 
    private Document dom;
    private boolean more = true;
    private String join = null;

    public StreamSnapshot(DataStream<DataType> stream, long start, long end, Interval interval, Aggregate aggregate, String join) {
        this.stream = stream;
        service = stream.getService();
        this.start = start;
        this.end = end;
        this.join = join;
        this.interval = interval == null ? Interval.None : interval;
        this.aggregate = aggregate == null ? Aggregate.None : aggregate;
        boolean noIntervalWithAgg = interval.equals(Interval.None) && !aggregate.equals(Aggregate.None);
        boolean noAggWithInterval = !interval.equals(Interval.None) && aggregate.equals(Aggregate.None);
        if (noAggWithInterval || noIntervalWithAgg) {
            throw new IllegalArgumentException("Aggregate and Interval must both be specified");
        }
    }

    public synchronized HttpResponse fetchNextChunk()
            throws IOException {
        try {
            URIBuilder builder = new URIBuilder();
            builder.setScheme(service.getScheme())
                .setHost(service.getHost())
                .setPort(service.getPort())
                .setPath("/ws/DataPoint/"+stream.getStreamName())
                .setParameter("startTime", ""+start)
                .setParameter("endTime", ""+end);

            // are doing a rollup?
            if (!Interval.None.equals(interval)) {
                builder.setParameter("rollupInterval", interval.name().toLowerCase());
                builder.setParameter("rollupMethod", aggregate.name().toLowerCase()); 
            }
            
            // is the timezone set?
            if (service.getTimezone() != null) {
                builder.setParameter("timezone", service.getTimezone());
            }
            // join other streams?
            if(join != null) {
                builder.setParameter("join", join);
            }
            // continue from previous call?
            if(dom != null) {
                NodeList nl = dom.getElementsByTagName("pageCursor");
                if(nl.getLength() > 0) {
                    String cursor = nl.item(0).getTextContent();
                    builder.setParameter("pageCursor", cursor);
                }
            }
            
            URI uri = builder.build();
            log.debug("query: "+uri.toString());
            HttpGet httpget = new HttpGet(uri);
            httpget.setHeader("Content-type", "text/xml; charset=utf-8");
            httpget.setHeader("Authorization", "Basic " + service.getAuthHeader());
            return service.httpclient.execute(httpget);
        } catch (URISyntaxException syntax) {
            log.error("URI Syntax exception: ", syntax);
        }
        return null;
    }

    /**
     * returns true if there is additional datapoints available in this time
     * period
     */
    public synchronized boolean hasNext() {
        if ((buffer == null || buffer.size() == 0) && more) {
            // empty buffer, we are either done or need to fetch more
            if (buffer == null)
                buffer = new LinkedList();
            try {
                // wait for next chunks response
                HttpResponse rsp = fetchNextChunk();
                int status = rsp.getStatusLine().getStatusCode();
                if (status == 401) {
                    throw new RuntimeException("Invalid credentials, HTTP 401");
                } else if (status != 200) {
                    log.error(EntityUtils.toString(rsp.getEntity()));
                    throw new RuntimeException("Unexpected status code: (" + status + ") " +
                            rsp.getStatusLine().getReasonPhrase());
                }
                // parse xml into dom
                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                DocumentBuilder db = dbf.newDocumentBuilder(); 
                dom = db.parse(rsp.getEntity().getContent());
                // get all the DataPoint elements
                NodeList points = dom.getElementsByTagName("DataPoint");
                // add all the data points to the buffer
                for (int i = 0; i < points.getLength(); i++) {
                    String dataPoint = DataStreamService.nodeToString(points.item(i));
                    DataPoint<DataType> dp = (DataPoint<DataType>) xstream.fromXML(dataPoint);
                    dp.setValueClass(stream.getValueClass());
                    buffer.add(dp);
                } 
                more = points.getLength() == 1000;
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
        if (!hasNext())
            throw new IndexOutOfBoundsException();
        DataPoint<DataType> ret = buffer.remove(0); 
        return ret;
    }

    public void remove() {
        // TODO record last read UUID and send an HTTP DELETE to support
        throw new RuntimeException("Not implemented");
    }

    /**
     * This is created for convience so can be used in for loops, but to meet
     * the iteratable contract we need to be able to iterate more then once,
     * this is inefficient but we will just create a new stream each time this
     * is called
     * 
     * TODO cache the results of the fetches so do not need to be refetched per
     * iteration
     */
    public Iterator<DataPoint<DataType>> iterator() {
        return new StreamSnapshot<DataType>(stream, start, end, interval, aggregate, join);
    }

    public String getJoin() {
        return join;
    }

    public void setJoin(String join) {
        this.join = join;
    }

}
