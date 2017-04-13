package com.cleo.clarify.control;

import java.util.List;

import com.cleo.clarify.control.methods.HostStatus;
import com.cleo.clarify.control.methods.ServiceDiscovery;
import com.cleo.clarify.control.pb.HostStatusReply.Host;
import com.cleo.clarify.control.pb.ServiceLocationReply.ServiceLocation;
import com.google.common.net.HostAndPort;
import com.orbitz.consul.Consul;
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.model.catalog.CatalogService;

public class ControlClient {
	
	private final Consul consul;
	
	private ControlClient(String address, int port) {
		this.consul = Consul.builder().withHostAndPort(HostAndPort.fromParts(address, port)).build();
	}
	
	public List<Host> hostStatus() {
		HostAndPort controlServer = findControlServer();
		return new HostStatus(controlServer.getHostText(), controlServer.getPort()).execute();
	}
	
	public List<ServiceLocation> discoverService(String serviceName) {
		HostAndPort controlServer = findControlServer();
		return new ServiceDiscovery(controlServer.getHostText(), controlServer.getPort())
				.forService(serviceName)
				.execute();
	}
	
	private HostAndPort findControlServer() {
		ConsulResponse<List<CatalogService>> serviceResponse = consul.catalogClient().getService("clarify-control");
		if (serviceResponse.getResponse().size() < 1) {
			throw new UnableToDiscoverControlServer();
		}
		CatalogService svc = serviceResponse.getResponse().get(0);
		return HostAndPort.fromParts(svc.getServiceAddress(), svc.getServicePort());
	}
		
	public static final Builder newBuilder() {
		return new Builder();
	}
	
	public static final class Builder {
		
		private String address;
		private int port;
		
		public Builder withDiscoveryAddress(String address) {
			this.address = address;
			return this;
		}
		
		public Builder withDiscoveryPort(int port) {
			this.port = port;
			return this;
		}
		
		public ControlClient build() {
			return new ControlClient(address, port);
		}
	}

}
