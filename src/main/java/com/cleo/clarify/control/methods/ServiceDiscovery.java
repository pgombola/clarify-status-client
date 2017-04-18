package com.cleo.clarify.control.methods;

import java.util.List;

import com.cleo.clarify.control.pb.ClarifyControlGrpc.ClarifyControlBlockingStub;
import com.cleo.clarify.control.pb.Service;
import com.cleo.clarify.control.pb.ServiceLocationReply;
import com.cleo.clarify.control.pb.ServiceLocationReply.ServiceLocation;

public class ServiceDiscovery implements ControlMethod<List<ServiceLocation>> {
	
	private final ClarifyControlBlockingStub blockingStub;
	private String serviceName = "";
	private String serviceTag = "";
	private boolean healthy = true;
	
		
	public ServiceDiscovery(ClarifyControlBlockingStub blockingStub) {
		this.blockingStub = blockingStub;
	}
	
	/**
	 * Sets the service name that should be discovered.
	 */
	public ServiceDiscovery forService(String name) {
		if (name == null || name.isEmpty()) throw new IllegalArgumentException("Service name must not be empty.");
		this.serviceName = name;
		return this;
	}
	
	/**
	 * Sets the tag name that should be used to filter.
	 */
	public ServiceDiscovery withTag(String tag) {
		if (tag == null || tag.isEmpty()) throw new IllegalArgumentException("Service tag must not be empty.");
		this.serviceTag = tag;
		return this;
	}
	
	/**
	 * Discover all services including unhealthy instances.
	 */
	public ServiceDiscovery allowUnhealthy() {
		this.healthy = false;
		return this;
	}

	@Override
	public List<ServiceLocation> execute() {
		if (serviceName == null || serviceName.isEmpty()) throw new IllegalStateException("Service name must be set.");
		Service service = Service.newBuilder()
			.setServiceName(serviceName)
			.setServiceTag(serviceTag)
			.setHealthy(healthy)
			.build();
		ServiceLocationReply reply = blockingStub.serviceLocation(service);
		return reply.getLocationsList();
	}

}
