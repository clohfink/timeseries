package com.digi.data.timeseries.examples;

import java.util.Scanner;

import com.digi.data.timeseries.DataPoint;
import com.digi.data.timeseries.DataStream;
import com.digi.data.timeseries.DataStreamService;

public class DisplayStreamContents { 
    
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
        System.out.println("====");

        DataStreamService service = DataStreamService.getServiceForHost(host, username, password);
        DataStream<?> stream = service.getStream(id);
         
        try {
            for (DataPoint point : stream.get(-1, -1)) {
                 System.out.println(point.getData());
            }
        } catch (Throwable e) {
        }
    }
}
