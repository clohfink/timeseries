# iDigi time series java client (in progress)
==================================
 
 This is a java wrapper to the data stream web services outlined [here](https://www.digi.com/wiki/developer/index.php/IDigi_Data_Streams)
 
## Installation

- Download

- Maven Repository: 

```xml
    <dependencies>
      ...
      <dependency>
        TODO
      </dependency>
      ...
    </dependencies>
```

## Usage

```java
    public static final DataStreamService service = 
            DataStreamService.getService("username", "password");
    // ...
         
    DataStream<Double> stream = service.getStream("myStream");
    long now = System.currentTimeMillis();
    long hourAgo = now - (1000 * 60 * 60);
    for (DataPoint<Double> data : stream.get(hourAgo, now)) {
        Date time = new Date(data.getTimestamp());
        System.out.println("Data point at "+ time + " = " + data.getValue());
    }
```