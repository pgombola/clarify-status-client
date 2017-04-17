package com.cleo.clarify.control.client;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.cleo.clarify.control.ControlServerException;
import com.cleo.clarify.control.pb.ClarifyControlGrpc;
import com.cleo.clarify.control.pb.HostStatusReply;
import com.cleo.clarify.control.pb.HostStatusReply.Builder;
import com.cleo.clarify.control.pb.HostStatusReply.Host;
import com.cleo.clarify.control.pb.HostStatusReply.Host.HostStatus;
import com.cleo.clarify.control.pb.HostStatusRequest;

import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;

public class HostStatusTest {
	
	@ClassRule public static final ControlServerResource controlResource = new ControlServerResource();
	@Rule public ExpectedException exception = ExpectedException.none();
	
	@AfterClass
	public static void shutdown() {
		controlResource.shutdown();
	}
	
	@Test
	public void hostStatus_size_3() {
		controlResource.registerAndStart(statusService(HostStatus.STARTED, HostStatus.STARTED, HostStatus.STARTED));
		
		assertThat(controlResource.controlClient().hostStatus().size(), equalTo(3));
	}
	
	@Test
	public void hostStatus_status_equal_started() {
		controlResource.registerAndStart(statusService(HostStatus.STARTED, HostStatus.STARTED, HostStatus.STARTED));
		
		for (Host h : controlResource.controlClient().hostStatus()) {
			assertThat(h.getStatus(), equalTo(HostStatus.STARTED));
		}
	}
	
	@Test
	public void hostStatus_reply_has_error() {
		controlResource.registerAndStart(new ClarifyControlGrpc.ClarifyControlImplBase() {

			@Override
			public void getHostStatus(HostStatusRequest request, StreamObserver<HostStatusReply> responseObserver) {
				responseObserver.onNext(HostStatusReply.newBuilder().setError("Error").build());
				responseObserver.onCompleted();
			}
		});
		exception.expect(ControlServerException.class);
		controlResource.controlClient().hostStatus();
		exception.expectMessage(equalTo("Error"));
	}
	
	@Test
	public void hostStatus_coordinatorLeader() {
		controlResource.registerAndStart(new ClarifyControlGrpc.ClarifyControlImplBase() {

			@Override
			public void getHostStatus(HostStatusRequest request, StreamObserver<HostStatusReply> responseObserver) {
				Host host = Host.newBuilder().setHostname("server-1").setStatus(HostStatus.STARTED).setCoordinatorLeader(true).build();
				responseObserver.onNext(HostStatusReply.newBuilder().addHosts(host).build());
				responseObserver.onCompleted();
			}
		});
		
		Host status = controlResource.controlClient().hostStatus().get(0);
		assertThat(status.getCoordinatorLeader(), equalTo(true));
	}
	
	private BindableService statusService(final HostStatus... status) {
		return new ClarifyControlGrpc.ClarifyControlImplBase() {

			@Override
			public void getHostStatus(HostStatusRequest request, StreamObserver<HostStatusReply> responseObserver) {
				Builder replyBuilder = HostStatusReply.newBuilder();
				int count = 1;
				for (HostStatus s : status) {
					replyBuilder.addHosts(Host.newBuilder().setHostname("server-" + count).setStatus(s).build());
					count++;
				}
				responseObserver.onNext(replyBuilder.build());
				responseObserver.onCompleted();
			}
			
		};
	}

}
