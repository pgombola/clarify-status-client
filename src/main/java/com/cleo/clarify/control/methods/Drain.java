package com.cleo.clarify.control.methods;

import com.cleo.clarify.control.pb.ClarifyControlGrpc.ClarifyControlBlockingStub;
import com.cleo.clarify.control.pb.DrainReply;
import com.cleo.clarify.control.pb.DrainRequest;
import com.cleo.clarify.control.pb.Node;

public class Drain implements ControlMethod<DrainReply> {
	
	private final ClarifyControlBlockingStub blockingStub;
	private String hostname;
	private boolean enabled = false;
	
	public Drain(ClarifyControlBlockingStub blockingStub) {
		this.blockingStub = blockingStub;
	}
	
	public Drain withHostname(String hostname) {
		this.hostname = hostname;
		return this;
	}
	
	public Drain enabled() {
		this.enabled = true;
		return this;
	}

	@Override
	public DrainReply execute() {
		if (hostname == null) throw new IllegalArgumentException("Hostname must not be empty");
		DrainRequest request = DrainRequest.newBuilder()
			.setNode(Node.newBuilder().setHostname(hostname).build())
			.setEnabled(enabled)
			.build();
		return blockingStub.drain(request);
	}

}
