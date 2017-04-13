package com.cleo.clarify.control.methods;

import java.util.List;

import com.cleo.clarify.control.pb.ClarifyControlGrpc;
import com.cleo.clarify.control.pb.ClarifyControlGrpc.ClarifyControlBlockingStub;
import com.cleo.clarify.control.pb.ServiceLocationReply;
import com.cleo.clarify.control.pb.ServiceLocationReply.ServiceLocation;
import com.cleo.clarify.control.pb.ServiceLocationRequest;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class ServiceDiscovery implements ControlMethod<List<ServiceLocation>> {
	
	private final ManagedChannel channel;
	private final ClarifyControlBlockingStub blockingStub;
	private String serviceName = "";
	private String serviceTag = "";
	private boolean healthy = true;
	
	public ServiceDiscovery(String host, int port) {
		this(ManagedChannelBuilder.forAddress(host, port).usePlaintext(true));
	}
	
	private ServiceDiscovery(ManagedChannelBuilder<?> channelBuilder) {
		this.channel = channelBuilder.build();
		this.blockingStub = ClarifyControlGrpc.newBlockingStub(channel);
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
	public ServiceDiscovery returnUnhealthy() {
		this.healthy = false;
		return this;
	}

	@Override
	public List<ServiceLocation> execute() {
		if (serviceName == null || serviceName.isEmpty()) throw new IllegalStateException("Service name must be set.");
		ServiceLocationRequest locationRequest = ServiceLocationRequest.newBuilder()
			.setServiceName(serviceName)
			.setServiceTag(serviceTag)
			.setHealthy(healthy)
			.build();
		ServiceLocationReply reply = blockingStub.getServiceLocation(locationRequest);
		return reply.getLocationsList();
	}

}
