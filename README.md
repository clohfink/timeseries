# iDigi time series java client (in progress)
==================================
 
 This is a java wrapper to the data stream web services outlined [here](https://www.digi.com/wiki/developer/index.php/IDigi_Data_Streams)

## Usage

```java
    public static final DataStreamService service = 
            DataStreamService.getService("username", "password");
    // ...
         
    DataStream<Double> stream = service.getStream("myStream");
    // get the current time in milliseconds
    long now = System.currentTimeMillis();
    // on hour ago in epoc ms
    long hourAgo = now - (1000 * 60 * 60);
    
    // fetch all the data points in last hour and iterate through them
    for (DataPoint<Double> data : stream.get(hourAgo, now)) {
        // convert timestamp into date to be more human readable
        Date time = new Date(data.getTimestamp());
        System.out.println("Data point at "+ time + " = " + data.getValue());
    }
```