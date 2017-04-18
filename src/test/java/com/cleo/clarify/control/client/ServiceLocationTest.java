package com.cleo.clarify.control.client;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.cleo.clarify.control.pb.ClarifyControlGrpc;
import com.cleo.clarify.control.pb.Service;
import com.cleo.clarify.control.pb.ServiceLocationReply;
import com.cleo.clarify.control.pb.ServiceLocationReply.Builder;
import com.cleo.clarify.control.pb.ServiceLocationReply.ServiceLocation;

import io.grpc.stub.StreamObserver;

public class ServiceLocationTest {

	@ClassRule public static final ControlServerResource controlResource = new ControlServerResource();
	@Rule public ExpectedException expectedException = ExpectedException.none();
	
	@AfterClass
	public static void shutdown() {
		controlResource.shutdown();
	}
	
	@Test
	public void no_services_registered_returns_empty() {
		registerService();
		
		List<ServiceLocation> locations = controlResource.controlClient().discoverService("coordinatorNode");
		
		assertThat(locations, empty());
	}
	
	@Test
	public void set_empty_service_throws_IllegalArg() {
		registerService();
		
		expectedException.expect(IllegalArgumentException.class);
		controlResource.controlClient().discoverService("");
	}
	
	@Test
	public void discovers_host_and_port() {
		final String svc = "svc1";
		registerService(svc);
		
		List<ServiceLocation> locations = controlResource.controlClient().discoverService(svc);
		
		assertThat(locations.size(), equalTo(1));
		assertHostAndPort(locations.get(0), svc, 1234L);
	}
	
	private void assertHostAndPort(ServiceLocation svc, String host, long port) {
		assertThat(svc.getServiceHost(), equalTo(host));
		assertThat(svc.getServicePort(), equalTo(port));
	}
	
	private void registerService(final String... services) {
		controlResource.registerAndStart(new ClarifyControlGrpc.ClarifyControlImplBase() {

			@Override
			public void serviceLocation(Service request, StreamObserver<ServiceLocationReply> responseObserver) {
				Builder replyBuilder = ServiceLocationReply.newBuilder();
				for (String svc : services) {
					replyBuilder.addLocations(
							ServiceLocation.newBuilder().setServiceHost(svc).setServicePort(1234).build());
				}
				responseObserver.onNext(replyBuilder.build());
				responseObserver.onCompleted();
			}
		});
	}
}
