package com.digi.data.timeseries;

import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.io.xml.StaxDriver;

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
    private static final Logger log = LoggerFactory.getLogger(DataStream.class);
    
    /**
    * Name of the stream
    */
    private String streamName;
    
    /**
    * iDigi password
    */
    private String password;
    
    /**
    * service used to create this datastream
    */
    private DataStreamService service;
    
    /**
    * class of the generic type of this stream
    */
    private Class<? extends DataType> valueClass;
    
    /**
    * Map of element:value for a data stream object
    */
    private Map<String, String> streamValues = null; 
    
    /** Used to serialize DataStream as a Map from xml */
    private static final XStream dsToMapXstream = new XStream(new StaxDriver());
    static {
        dsToMapXstream.alias("DataStream", Map.class);
        dsToMapXstream.registerConverter(new DataStreamMapEntryConverter());
    }
    
    /*
    * package only constructor called from DataStreamService, fetches the data type
    * to use as the valueClass
    */
    DataStream(String streamName, DataStreamService service) {
        this.streamName = streamName;
        this.service = service; 
        try {
            refresh();
            this.valueClass = (Class<? extends DataType>) this.getDataType();
        } catch (Exception e) {
            this.valueClass = (Class<? extends DataType>) String.class;
        }
        
    }
    
    /*
    * package only constructor called from DataStreamService
    */
    DataStream(String streamName, Class<? extends DataType> valueClass, 
            DataStreamService service) {
        this.streamName = streamName;
        this.service = service; 
        this.valueClass = valueClass;
    }
    
    /**
    * Fetch the aggregate for a given interval between two times. The returned
    * snapshot can be used as iterator or iterable in loop, ie
    * 
    * <pre>
    * DataStream&lt;Float> steram = ...
    * long now = System.currentTimeMillis();
    * for(DataPoint&lt;Float> data : stream.get(Aggregate.Sum, Interval.Hour, 0L, now)) {
    *       float sumForHour = data.getValue();
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
    
    public StreamSnapshot<DataType> get(Aggregate aggregate, Interval interval, long start, long end, String joined) {
        StreamSnapshot<DataType> ret = new StreamSnapshot<DataType>(this, start, end, interval, aggregate);
        ret.setJoin(joined);
        return ret;
    }
    
    /**
     * Fetch the data points between two times. The returned snapshot can be
     * used as iterator or iterable in loop, ie
     * 
     * <pre>
     * DataStream&lt;Float> steram = ...
     * for(DataPoint&lt;Float> data : stream.get(0, System.currentTimeMillis()) ){
     *     float dataPointValue = data.getValue();
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
        return get(Aggregate.None, Interval.None, start, end);
    }
    

    /**
     * Convenience method to fetch all data points in a stream, this is the equivalent to 
     * <pre>
     *  stream.get(-1, -1);
     * </pre>  
     */
    public StreamSnapshot<DataType> getAll() {
        return get(-1, -1);
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

    public String getUnits() throws DataServiceException {
        if(streamValues == null) {
            this.refresh();
        }
        return streamValues.get("units");
    }
    
    /**
    * 
    * @return
    * @throws DataServiceException
    */
    public String getDescription() throws DataServiceException {
        if(streamValues == null) {
            this.refresh();
        }
        return streamValues.get("description");
    }
    
    /**
    * class of the generic type of this stream
    */
    public Class<? extends DataType> getValueClass() {
        return valueClass;
    }
 
    /**
    * returns whatever class represents the datatype listed in the
    * streams metadata.  This is from the "dataType" element returned
    * from the /ws/DataStream web service.  If refresh() has been called
    * it will use the cached value from it, otherwise it will make
    * the web service call
    * 
    * @return
    * @throws DataServiceException
    */
    public Class getDataType() throws DataServiceException {
        Class ret = null;
        if(streamValues == null) {
            this.refresh();
        }
        String type = streamValues.get("dataType").toLowerCase();
        if(type.equals("integer")) {
            ret = Integer.class;
        } else if(type.equals("double")) {
            ret = Double.class;
        } else if(type.equals("float")) {
            ret = Float.class;
        } else if(type.equals("long")) {
            ret = Long.class;
        } else {
            ret = String.class;
        }
        return ret;
    }
    
    /**
    * fetches the current values of the data stream meta data, ie dataType, description, units
    * 
    * @throws DataServiceException
    */
    public void refresh() throws DataServiceException {  
        try {
          URIBuilder builder = new URIBuilder();
            builder.setScheme(service.getScheme())
                .setHost(service.getHost())
                .setPort(service.getPort())
                .setPath("/ws/DataStream/"+streamName);
            
            HttpGet httpget = new HttpGet(builder.build());
            httpget.setHeader("Content-type", "text/xml; charset=utf-8");
            httpget.setHeader("Authorization", "Basic " + service.getAuthHeader());
            
            HttpResponse rsp = DataStreamService.httpclient.execute(httpget); 

            int status = rsp.getStatusLine().getStatusCode();
            if (status == 401) {
                throw new IOException("Invalid credentials, HTTP 401");
            } else if (status != 200) {
                log.error(EntityUtils.toString(rsp.getEntity()));
                throw new IOException("Unexpected status code: (" + status + ") " +
                        rsp.getStatusLine().getReasonPhrase());
            } 
            // parse the data stream(s)
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();  
            Document dom = db.parse(rsp.getEntity().getContent());
            // get all the DataStream elements
            NodeList streams = dom.getElementsByTagName("DataStream");
            // if none of the xml results doesnt contain any dataStream elements
            if(streams.getLength() < 1) {
                throw new DataServiceException("Cannot find matching data stream");
            }
            // use streamValues as a flag for the loop and a check incase none of the
            // returned streams match (ie its a parent path)
            streamValues = null;
            // only use the one that matches the name of this stream
            for (int i = 0; i < streams.getLength() && streamValues == null; i++) {
                String dataStream = DataStreamService.nodeToString(streams.item(i));
                Map<String,String> dsMap = (Map<String,String>) dsToMapXstream.fromXML(dataStream);
                if(dsMap.get("streamId").equals(this.streamName)) {
                    this.streamValues = dsMap;
                }
            }
            // check if no matching streams were found
            if(streamValues == null) {
                throw new DataServiceException("Cannot find matching data stream");
            }
            
        } catch (IOException e) {
            throw new DataServiceException("IOException: " + e.getMessage(), e);
        } catch (SAXException e) {
            throw new DataServiceException("Sax error parsing document: " + e.getMessage(), e);
        } catch (ParserConfigurationException e) {
            throw new DataServiceException("Cannot create DocumentBuilder: " + e.getMessage(), e);
        } catch (URISyntaxException e) {
          throw new DataServiceException("Invalid URI created (/ws/DataStream/"+streamName+"): "
                  + e.getMessage(), e);
        }
    } 

    /*
    * internal mechanism to get the service used to create this stream
    */
    DataStreamService getService() {
        return service;
    }
    
    @Override
    public String toString() {
        return "DataStream [streamName=" + streamName + ", valueClass=" + valueClass + ", streamValues=" + streamValues
                + "]";
    }

    /*
    * used by xstream to convert data stream to and from xml
    */
    private static class DataStreamMapEntryConverter implements Converter{
        public boolean canConvert(Class clazz) {
            return AbstractMap.class.isAssignableFrom(clazz);
        }

        public void marshal(Object value, HierarchicalStreamWriter writer, MarshallingContext context) {
            AbstractMap<String,String> map = (AbstractMap<String,String>) value;
            for (Entry<String,String> entry : map.entrySet()) {
                writer.startNode(entry.getKey().toString());
                writer.setValue(entry.getValue().toString());
                writer.endNode();
            }
        }

        public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {
            Map<String, String> map = new HashMap<String, String>();

            while(reader.hasMoreChildren()) {
                reader.moveDown();
                String name = reader.getNodeName();
                if(!name.equals("currentValue")) { 
                   map.put(name, reader.getValue());
                }
                reader.moveUp();
            }
            return map;
        }
    }
}
