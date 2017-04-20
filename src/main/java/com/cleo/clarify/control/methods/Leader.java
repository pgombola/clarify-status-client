package com.cleo.clarify.control.methods;

import com.cleo.clarify.control.LeaderNotFound;
import com.cleo.clarify.control.pb.ClarifyControlGrpc.ClarifyControlBlockingStub;
import com.cleo.clarify.control.pb.Service;
import com.cleo.clarify.control.pb.ServiceLocationReply;
import com.cleo.clarify.control.pb.ServiceLocationReply.ServiceLocation;

public class Leader implements ControlMethod<ServiceLocation> {

	private final ClarifyControlBlockingStub blockingStub;
	private String serviceName;
	
	public Leader(ClarifyControlBlockingStub blockingStub) {
		this.blockingStub = blockingStub;
	}
	
	public Leader forServiceName(String serviceName) {
		if (serviceName == null || serviceName.isEmpty()) throw new IllegalArgumentException("serviceName must not be empty");
		this.serviceName = serviceName;
		return this;
	}
	
	@Override
	public ServiceLocation execute() throws LeaderNotFound {
		ServiceLocationReply reply = blockingStub.leader(Service.newBuilder().setServiceName(serviceName).build());
		if (reply.getLocationsCount() > 0 && isValid(reply.getLocations(0))) {
			return reply.getLocations(0);
		} else {
			throw new LeaderNotFound(serviceName);
		}
	}
	
	private boolean isValid(ServiceLocation location) {
		return location.getServiceHost() != null 
				&& !location.getServiceHost().isEmpty() 
				&& location.getServicePort() > 0;
	}

}
