package com.cleo.clarify.control.methods;

import com.cleo.clarify.control.pb.Job;
import com.cleo.clarify.control.pb.StopReply;
import com.cleo.clarify.control.pb.ClarifyControlGrpc.ClarifyControlBlockingStub;

public class Stop implements ControlMethod<StopReply> {

	private final ClarifyControlBlockingStub blockingStub;
	private String jobName;
	
	public Stop(ClarifyControlBlockingStub blockingStub) {
		this.blockingStub = blockingStub;
	}
	
	public Stop withJob(String jobName) {
		this.jobName = jobName;
		return this;
	}
	
	@Override
	public StopReply execute() {
		if (jobName == null || jobName.isEmpty()) throw new IllegalArgumentException("Job name must not be empty");
		return blockingStub.stop(Job.newBuilder().setJobName(jobName).build());
	}

}
