package com.cleo.clarify.control;

import java.util.List;

import com.cleo.clarify.control.methods.Drain;
import com.cleo.clarify.control.methods.HostStatus;
import com.cleo.clarify.control.methods.Leader;
import com.cleo.clarify.control.methods.ServiceDiscovery;
import com.cleo.clarify.control.methods.Stop;
import com.cleo.clarify.control.pb.ClarifyControlGrpc;
import com.cleo.clarify.control.pb.ClarifyControlGrpc.ClarifyControlBlockingStub;
import com.cleo.clarify.control.pb.DrainReply;
import com.cleo.clarify.control.pb.NodeStatusReply.NodeDetails;
import com.cleo.clarify.control.pb.ServiceLocationReply.ServiceLocation;
import com.cleo.clarify.control.pb.StopReply;
import com.google.common.net.HostAndPort;
import com.orbitz.consul.Consul;
import com.orbitz.consul.ConsulException;
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.model.health.Service;
import com.orbitz.consul.model.health.ServiceHealth;
import com.orbitz.consul.option.ImmutableCatalogOptions;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.internal.DnsNameResolverProvider;
import io.grpc.netty.NettyChannelBuilder;

public class ControlClient {
	
	private final Consul consul;
	
	private ControlClient(String address, int port) throws InvalidDiscoveryAddress {
		try {
			this.consul = Consul.builder().withHostAndPort(HostAndPort.fromParts(address, port)).build();
		} catch (ConsulException e) {
			throw new UnableToDiscoverControlServer();
		}
	}
	
	public List<NodeDetails> nodeStatus() {
		ClarifyControlBlockingStub stub = createBlockingStub();
		return new HostStatus(stub).execute();
	}
	
	public List<ServiceLocation> discoverService(String serviceName) {
		return discoverService(serviceName, null, false);
	}
	
	public List<ServiceLocation> discoverService(String serviceName, String serviceTag) {
		return discoverService(serviceName, serviceTag, false);
	}
	
	public List<ServiceLocation> discoverService(String serviceName, String serviceTag, boolean allowUnhealthy) {
		ClarifyControlBlockingStub stub = createBlockingStub();
		ServiceDiscovery discovery = new ServiceDiscovery(stub)
			.forService(serviceName);
		if (serviceTag != null && !serviceTag.isEmpty()) {
			discovery.withTag(serviceTag);
		}
		if (allowUnhealthy) {
			discovery.allowUnhealthy();
		}
		return discovery.execute();
	}
	
	public DrainReply drain(String hostname, boolean enabled) {
		Drain d = new Drain(createBlockingStub()).withHostname(hostname);
		if (enabled) d.enabled();
		return d.execute();
	}
	
	public StopReply stop(String jobName) {
		return new Stop(createBlockingStub()).withJob(jobName).execute();
	}
	
	public ServiceLocation leader(String serviceName) throws LeaderNotFound {
		return new Leader(createBlockingStub()).forServiceName(serviceName).execute();
	}
		
	private ClarifyControlBlockingStub createBlockingStub() {
		HostAndPort controlServer = findControlServer();
		ManagedChannelBuilder<?> channelBuilder = NettyChannelBuilder
				.forAddress(controlServer.getHostText(), controlServer.getPort())
				.usePlaintext(true)
				.nameResolverFactory(new DnsNameResolverProvider());
		Channel channel = channelBuilder.build();
		return ClarifyControlGrpc.newBlockingStub(channel);
	}

	private HostAndPort findControlServer() {
		ConsulResponse<List<ServiceHealth>> serviceResponse = 
				consul.healthClient().getHealthyServiceInstances(
						"clarify-control", 
						ImmutableCatalogOptions.builder().tag("grpc").build());
		if (serviceResponse.getResponse().size() < 1) {
			throw new UnableToDiscoverControlServer();
		}
		Service svc = serviceResponse.getResponse().get(0).getService();
		return HostAndPort.fromParts(svc.getAddress(), svc.getPort());
	}
		
	public static final Builder newBuilder() {
		return new Builder();
	}
	
	public static final class Builder {
		
		private String address;
		private int port;
		
		private Builder() { }
		
		public Builder withDiscoveryAddress(String address) {
			this.address = address;
			return this;
		}
		
		public Builder withDiscoveryPort(int port) {
			this.port = port;
			return this;
		}
		
		public ControlClient build() throws InvalidDiscoveryAddress {
			return new ControlClient(address, port);
		}
	}

}
