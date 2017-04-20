package com.cleo.clarify.control.client;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.containsString;

import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.cleo.clarify.control.LeaderNotFound;
import com.cleo.clarify.control.pb.ClarifyControlGrpc;
import com.cleo.clarify.control.pb.Service;
import com.cleo.clarify.control.pb.ServiceLocationReply;
import com.cleo.clarify.control.pb.ServiceLocationReply.ServiceLocation;

import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;

public class LeaderTest {
	
	@ClassRule public static final ControlServerResource controlResource = new ControlServerResource();
	@Rule public ExpectedException exception = ExpectedException.none();
	
	@AfterClass
	public static void shutdown() {
		controlResource.shutdown();
	}
	
	@Test
	public void leader_discovered() {
		controlResource.registerAndStart(leaderRpc("127.0.0.1:8888"));
		
		ServiceLocation location = controlResource.controlClient().leader("coordinator");
		
		assertThat(location.getServiceName(), equalTo("coordinator"));
		assertThat(location.getServiceHost(), equalTo("127.0.0.1"));
		assertThat(location.getServicePort(), equalTo(8888L));
	}
	
	@Test
	public void leader_not_found() {
		controlResource.registerAndStart(leaderRpc());
		
		exception.expect(LeaderNotFound.class);
		controlResource.controlClient().leader("coordinator");
		
		exception.expectMessage(containsString("coordinator"));
	}
	
	private BindableService leaderRpc(final String... hostAndPort) {
		return new ClarifyControlGrpc.ClarifyControlImplBase() {

			@Override
			public void leader(Service request, StreamObserver<ServiceLocationReply> responseObserver) {
				ServiceLocationReply.Builder replyBuilder = ServiceLocationReply.newBuilder();
				for (String hp : hostAndPort) {
					String[] hpSplit = hp.split(":");
					replyBuilder.addLocations(
							ServiceLocation.newBuilder()
							.setServiceName(request.getServiceName())
							.setServiceHost(hpSplit[0])
							.setServicePort(Long.valueOf(hpSplit[1]))
							.build());
				}
				responseObserver.onNext(replyBuilder.build());
				responseObserver.onCompleted();
			}
		};
	}

}
