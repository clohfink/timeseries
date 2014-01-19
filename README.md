## DeviceCloud by Etherios time series client 

 This is a java wrapper to the data stream web services outlined [here](https://www.digi.com/wiki/developer/index.php/IDigi_Data_Streams)
 
### Installation

TODO

### Usage

You can create a DataStreamService that will be used to store authentication details.  It is recommended to create this with spring or save it
in a static final variable.

```java 
    public static final DataStreamService service = 
            DataStreamService.getService("username", "password"); 
```

Here is a example of getting all the data points for the last hour and printing them out.

```java  
    // get the stream of data points labeled "myStream"     
    DataStream<Double> stream = service.getStream("myStream");
    
    // get the current time in milliseconds
    long now = System.currentTimeMillis(); 
    
    // fetch all the data points in last hour and iterate through them
    for (DataPoint<Double> data : stream.get(now - (1000 * 60 * 60), now)) {
        // convert timestamp into date to be more human readable
        Date time = new Date(data.getTimestamp());
        System.out.println("Data point at "+ time + " = " + data.getValue());
    }
```

Here is an example that checks the value of a stream never exceeded a value

```java 
    // get the stream of data points labeled "myStream".
    DataStream<Float> stream = service.getStream("myStream");
    // print meta data on stream
    System.out.println(stream);

    // Iterate through all the streams datapoints and print to std out
    for (DataPoint<Float> point : stream.getAll()) {
         assert point.getValue() < MAX_VALUE;
    }  
```

That worked but it was inefficient to pull everything down so we can use the "MAX" aggregate to get the largest value per day and verify
it was never exceeded

```java 
    // get the stream of data points labeled "myStream".
    DataStream<Float> stream = service.getStream("myStream"); 

    // Iterate through all the streams datapoints and print to std out
    for (DataPoint<Float> point : stream.get(Aggregate.Max, Interval.Day, -1, -1)) {
         assert point.getValue() < MAX_VALUE;
    }  
```
