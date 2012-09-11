package com.digi.data.timeseries.examples;

import java.awt.Color;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.time.Hour;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RectangleInsets;
import org.jfree.ui.RefineryUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digi.data.timeseries.DataPoint;
import com.digi.data.timeseries.DataServiceException;
import com.digi.data.timeseries.DataStream;
import com.digi.data.timeseries.DataStreamService;
import com.digi.data.timeseries.Interval;

@SuppressWarnings({ "serial", "deprecation" })
public class GraphStream extends ApplicationFrame {
    private static final Logger log = LoggerFactory.getLogger(GraphStream.class);

    private DataStreamService service; 
    private DataStream<Double> stream;

    public GraphStream(final String title, String host, String username, String password, String streamId) throws DataServiceException {
        super(title);
        service = DataStreamService.getServiceForHost(host, username, password);
        stream = service.getStream(streamId, Double.class);
        
        TimeSeries s1 = new TimeSeries(stream.getStreamName(), Hour.class);
        try {
            for (DataPoint<Double> point : stream.getAverages(Interval.Hour, -1, -1)) {
                s1.add(new Hour(new Date(point.getTimestamp())), point.getValue());
            }
        } catch (Throwable e) {
        }
        TimeSeriesCollection dataset = new TimeSeriesCollection();
        dataset.addSeries(s1);

        dataset.setDomainIsPointsInTime(true);

        JFreeChart chart = ChartFactory.createTimeSeriesChart(stream.getDescription(), "Date", stream.getUnits(),
                dataset, true, true, false);

        chart.setBackgroundPaint(Color.white);

        XYPlot plot = (XYPlot) chart.getPlot();
        plot.setBackgroundPaint(Color.decode("#ffffff"));
        plot.setDomainGridlinePaint(Color.decode("#00A779"));
        plot.setRangeGridlinePaint(Color.decode("#00A779"));

        plot.setAxisOffset(new RectangleInsets(5.0, 5.0, 5.0, 5.0));
        plot.setDomainCrosshairVisible(true);
        plot.setRangeCrosshairVisible(true);

        XYItemRenderer r = plot.getRenderer();
        if (r instanceof XYLineAndShapeRenderer) {
            XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) r;
            renderer.setSeriesPaint(0, Color.decode("#48DD00"));
        }

        DateAxis axis = (DateAxis) plot.getDomainAxis();
        axis.setDateFormatOverride(new SimpleDateFormat("MMM-yyyy"));

        final ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(500, 270));
        setContentPane(chartPanel);
    }

    public static void main(final String[] args) throws Exception {
        Scanner in = new Scanner(System.in);
        System.out.println("----");
        System.out.print("Enter Host (ie my.idigi.com, or developer.idigi.com): ");
        String host = in.nextLine();
        System.out.print("Enter Username: ");
        String username = in.nextLine();
        System.out.print("Enter password: ");
        String password = in.nextLine();
        System.out.print("Enter Stream Id: ");
        String id = in.nextLine(); 

        final GraphStream demo = new GraphStream("Dia Example graph", host, username, password, id);
        demo.pack();
        RefineryUtilities.centerFrameOnScreen(demo);
        demo.setVisible(true);
    }

}
