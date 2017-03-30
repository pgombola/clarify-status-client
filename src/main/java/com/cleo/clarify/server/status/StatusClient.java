package com.cleo.clarify.server.status;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.List;

import com.cleo.clarify.clusterstatus.pb.ClarifyStatusGrpc;
import com.cleo.clarify.clusterstatus.pb.ClarifyStatusGrpc.ClarifyStatusBlockingStub;
import com.cleo.clarify.clusterstatus.pb.HostStatusReply;
import com.cleo.clarify.clusterstatus.pb.HostStatusReply.Host;
import com.cleo.clarify.clusterstatus.pb.HostStatusRequest;
import com.orbitz.consul.Consul;
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.model.catalog.CatalogService;

public class StatusClient {
	
	private final ManagedChannel channel;
	private final ClarifyStatusBlockingStub blockingStatusStub;
	
	public StatusClient(String host, int port) {
		this(ManagedChannelBuilder.forAddress(host, port).usePlaintext(true));
	}
	
	public StatusClient(ManagedChannelBuilder<?> channelBuilder) {
		this.channel = channelBuilder.build();
		this.blockingStatusStub = ClarifyStatusGrpc.newBlockingStub(channel);
	}
	
	public List<Host> status() {
		HostStatusRequest request = HostStatusRequest.newBuilder().setJobName("clarify").build();
		HostStatusReply reply = blockingStatusStub.getHostStatus(request);
		if (!reply.getError().isEmpty()) System.out.println(reply.getError());
		return reply.getHostsList();
	}

	public static void main(String[] args) {
		String consulAddr = findPropertyOrError(args, "consul.addr");
		Consul consul = Consul.builder().withUrl(String.format("http://%s", consulAddr)).build();
		ConsulResponse<List<CatalogService>> serviceResponse = consul.catalogClient().getService("clarify-status");
		if (serviceResponse.getResponse().size() < 1) {
			System.out.println("Couldn't find clarify-status service.");
			System.exit(1);
		}
		CatalogService svc = serviceResponse.getResponse().get(0);
		StatusClient client = new StatusClient(svc.getServiceAddress(), svc.getServicePort());
		List<Host> hosts = client.status();
		for (Host h : hosts) {
			System.out.println(String.format("Hostname: %s; Status: %s", h.getHostname(), h.getStatus()));
		}
	}
	
	private static final String findPropertyOrError(String[] args, String property) {
		String value = null;
		for (String arg : args) {
			if (arg.contains(property)){
				value = arg.substring(arg.indexOf("=") + 1);
			}
		}
		if (value == null || value.isEmpty()) {
			throw new IllegalArgumentException(String.format("Must provide %s argument.", property));
		} else {
			return value;
		}
	}
}
