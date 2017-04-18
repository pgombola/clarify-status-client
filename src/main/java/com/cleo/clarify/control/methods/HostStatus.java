package com.cleo.clarify.control.methods;

import java.util.List;

import com.cleo.clarify.control.ControlServerException;
import com.cleo.clarify.control.pb.ClarifyControlGrpc.ClarifyControlBlockingStub;
import com.cleo.clarify.control.pb.Job;
import com.cleo.clarify.control.pb.NodeStatusReply;
import com.cleo.clarify.control.pb.NodeStatusReply.NodeDetails;

public class HostStatus implements ControlMethod<List<NodeDetails>>{
	
	private final ClarifyControlBlockingStub blockingStatusStub;
	
	public HostStatus(ClarifyControlBlockingStub blockingStub) {
		this.blockingStatusStub = blockingStub;
	}
	
	@Override
	public List<NodeDetails> execute() {
		Job job = Job.newBuilder().setJobName("clarify").build();
		NodeStatusReply reply = blockingStatusStub.nodeStatus(job); 
		if (!reply.getError().isEmpty()) {
			throw new ControlServerException(reply.getError());
		}
		return reply.getDetailsList();
	}

}
