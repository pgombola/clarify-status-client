package com.cleo.clarify.control.methods;

import java.util.List;

import com.cleo.clarify.control.ControlServerException;
import com.cleo.clarify.control.pb.ClarifyControlGrpc;
import com.cleo.clarify.control.pb.ClarifyControlGrpc.ClarifyControlBlockingStub;
import com.cleo.clarify.control.pb.HostStatusReply;
import com.cleo.clarify.control.pb.HostStatusReply.Host;
import com.cleo.clarify.control.pb.HostStatusRequest;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class HostStatus implements ControlMethod<List<Host>>{
	
	private final ManagedChannel channel;
	private final ClarifyControlBlockingStub blockingStatusStub;
	
	public HostStatus(String host, int port) {
		this(ManagedChannelBuilder.forAddress(host, port).usePlaintext(true));
	}
	
	public HostStatus(ManagedChannelBuilder<?> channelBuilder) {
		this.channel = channelBuilder.build();
		this.blockingStatusStub = ClarifyControlGrpc.newBlockingStub(channel);
	}
	
	@Override
	public List<Host> execute() {
		HostStatusRequest request = HostStatusRequest.newBuilder().setJobName("clarify").build();
		HostStatusReply reply = blockingStatusStub.getHostStatus(request); 
		if (!reply.getError().isEmpty()) {
			throw new ControlServerException(reply.getError());
		}
		return reply.getHostsList();
	}

}
