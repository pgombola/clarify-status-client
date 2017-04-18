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
import com.cleo.clarify.control.pb.ClarifyStatus;
import com.cleo.clarify.control.pb.Job;
import com.cleo.clarify.control.pb.Node;
import com.cleo.clarify.control.pb.NodeStatusReply;
import com.cleo.clarify.control.pb.NodeStatusReply.Builder;
import com.cleo.clarify.control.pb.NodeStatusReply.NodeDetails;

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
		controlResource.registerAndStart(statusRpc(ClarifyStatus.NODE_ALLOC_STARTED, ClarifyStatus.NODE_ALLOC_STARTED, ClarifyStatus.NODE_ALLOC_STARTED));
		
		assertThat(controlResource.controlClient().nodeStatus().size(), equalTo(3));
	}
	
	@Test
	public void hostStatus_status_equal_started() {
		controlResource.registerAndStart(statusRpc(ClarifyStatus.NODE_ALLOC_STARTED, ClarifyStatus.NODE_ALLOC_STARTED, ClarifyStatus.NODE_ALLOC_STARTED));
		
		for (NodeDetails n : controlResource.controlClient().nodeStatus()) {
			assertThat(n.getStatus(), equalTo(ClarifyStatus.NODE_ALLOC_STARTED));
		}
	}
	
	@Test
	public void hostStatus_reply_has_error() {
		controlResource.registerAndStart(new ClarifyControlGrpc.ClarifyControlImplBase() {

			@Override
			public void nodeStatus(Job request, StreamObserver<NodeStatusReply> responseObserver) {
				responseObserver.onNext(NodeStatusReply.newBuilder().setError("error").build());
				responseObserver.onCompleted();
			}
		});
		
		exception.expect(ControlServerException.class);
		controlResource.controlClient().nodeStatus();
		
		exception.expectMessage(equalTo("Error"));
	}
	
	@Test
	public void hostStatus_coordinatorLeader() {
		controlResource.registerAndStart(new ClarifyControlGrpc.ClarifyControlImplBase() {

			@Override
			public void nodeStatus(Job request, StreamObserver<NodeStatusReply> responseObserver) {
				NodeDetails details = NodeDetails.newBuilder()
						.setNode(Node.newBuilder().setHostname("server-1").build())
						.setCoordinatorLeader(true).build();
				responseObserver.onNext(NodeStatusReply.newBuilder().addDetails(details).build());
				responseObserver.onCompleted();
			}
		});
		
		NodeDetails details = controlResource.controlClient().nodeStatus().get(0);
		assertThat(details.getCoordinatorLeader(), equalTo(true));
	}
	
	private BindableService statusRpc(final ClarifyStatus... status) {
		return new ClarifyControlGrpc.ClarifyControlImplBase() {

			@Override
			public void nodeStatus(Job request, StreamObserver<NodeStatusReply> responseObserver) {
				Builder replyBuilder = NodeStatusReply.newBuilder();
				int count = 1;
				for (ClarifyStatus s : status) {
					replyBuilder.addDetails(
							NodeDetails.newBuilder()
								.setNode(Node.newBuilder().setHostname("server-" + count).build())
								.setStatus(s).build());
					count++;
				}
				responseObserver.onNext(replyBuilder.build());
				responseObserver.onCompleted();
			}
		};
	}

}
