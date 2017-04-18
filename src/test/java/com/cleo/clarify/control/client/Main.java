package com.cleo.clarify.control.client;

import java.util.List;

import com.cleo.clarify.control.ControlClient;
import com.cleo.clarify.control.pb.ServiceLocationReply.ServiceLocation;

public class Main {

	public static void main(String[] args) {
		ControlClient client = ControlClient.newBuilder().withDiscoveryAddress("10.10.20.31").withDiscoveryPort(8500).build();
		List<ServiceLocation> locations = client.discoverService("coordinatorNode");
		for (ServiceLocation l : locations) {
			System.out.println(l);
		}
	}
	
}
