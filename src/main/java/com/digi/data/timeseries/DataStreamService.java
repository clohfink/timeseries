package com.digi.data.timeseries;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
/**
 * Service for retrieving DataStream objects which can be used to query data over ranges
 * or aggregates.
 */
public class DataStreamService {
    private static final Logger log = LoggerFactory.getLogger(DataStreamService.class);
    static final HttpClient httpclient;
    static {
        HttpParams params = new BasicHttpParams();
        SchemeRegistry registry = new SchemeRegistry();
        registry.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
        registry.register(new Scheme("https", 443, SSLSocketFactory.getSocketFactory()));
        ClientConnectionManager ccm = new PoolingClientConnectionManager(registry);
        
        httpclient = new DefaultHttpClient(ccm, params);
    } 
    private String scheme = "https";
    private int port = 443; 
    private String host;
    private String auth;
    private String timezone = null;
    
    private static Map<String, DataStreamService> services = new HashMap();
    
    private DataStreamService(String username, String password, String host) {
        super(); 
        this.host = host;
        String userpassword = username + ":" + password;
        this.auth = Base64.encodeBase64String(userpassword.getBytes()).trim(); 
        System.err.println(this);
    }
    
    /**
    * Factory method for retrieving a DataStreamService. The username and
    * password will be used in all sequential queries related to DataStreams
    * created by this service.
    * 
    * If using Spring this is a good place to inject a bean for autowiring
    * <pre>
    *     &lt;bean id="dataStreamService" class="com.digi.data.timeseries.DataStreamService"
    *                 factory-method="getService"> 
    *         &lt;constructor-arg ref="${username}" />
    *         &lt;constructor-arg ref="${password}" />
    *  &lt;/bean>
    * </pre>
    * 
    * @param host
    * @param username
    * @param password
    * @return
    */
    public static synchronized DataStreamService getService(String username, String password) {
        return new DataStreamService(username, password, "my.idigi.com");
    }

    /**
    * @see getService
    */
    public static DataStreamService getServiceForHost(String host, String username, String password) { 
        String key = host+"::"+username+"::"+DigestUtils.md5Hex(password);
        if(services.get(key) == null) {
            services.put(key, new DataStreamService(username, password, host));
        }
        return services.get(key);
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
    
    /**
    * Set if http requests for this service should use HTTP or HTTPS.  By default
    * it will use HTTPS
    * 
    * @param isSecure
    */
    public void setSecureScheme(boolean isSecure) {
       this.scheme = isSecure ? "HTTPS" : "HTTP";
    }
      
    /**
     * Can override the port used for the http[s] requests.  Defaults to 443
     * 
     * @param isSecure
     */
    public void setPort(int port) {
       this.port = port;
    }

    /**
    * retrieve the scheme (http/https) used for this service
    */
    public String getScheme() {
        return scheme;
    }

    /**
     * retrieves port used to make the http[s] requests
     */
    public int getPort() {
        return port;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }
}
