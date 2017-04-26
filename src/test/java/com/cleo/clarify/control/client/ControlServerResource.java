package com.cleo.clarify.control.client;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.ServerSocket;

import org.junit.rules.ExternalResource;

import com.cleo.clarify.control.ControlClient;
import com.google.common.net.HostAndPort;
import com.orbitz.consul.Consul;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import com.pszymczyk.consul.ConsulProcess;
import com.pszymczyk.consul.ConsulStarterBuilder;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

public class ControlServerResource extends ExternalResource {
	
	private Server grpcServer;
	private ConsulProcess consulProc;
	private Consul consul;
	
	@Override
	protected void before() throws Throwable {
		super.before();
		if (consulProc == null) {
			consulProc = ConsulStarterBuilder.consulStarter().build().start();
			consul = Consul.builder().withHostAndPort(HostAndPort.fromParts("localhost", consulProc.getHttpPort())).build();
		}
		else {
			consulProc.reset();
		}
	}
	
	@Override
	protected void after() {
		super.after();
		if (grpcServer != null) {
			grpcServer.shutdownNow();
		}
	}
	
	/**
	 * Shutdown <b>MUST</b> be called in order to shutdown the embedded Consul process.
	 */
	public void shutdown() {
		if (consulProc != null) {
			consulProc.close();
		}
	}
	
	/**
	 * Registers the provided service with a gRPC server started on a random port.
	 * The registered service is also added to embedded Consul as "clarify-control" 
	 * to become discoverable by the ControlClient.
	 * @param service The service registered with this gRPC server.
	 * @return The random port the gRPC server is listening on.
	 */
	public void registerAndStart(BindableService service) {
		int port = getFreePort();
		ServerBuilder<?> builder = ServerBuilder.forPort(port);
		builder.addService(service);
		grpcServer = builder.build();
		try {
			grpcServer.start();
		} catch (IOException e) {
			fail("Error starting grpc server.");
		}
		registerServiceInConsul();
	}
	
	/**
	 * @return ControlClient that is configured to connect to the embedded consul instance 
	 */
	public ControlClient controlClient() {
		return ControlClient.newBuilder().withDiscoveryAddress("localhost").withDiscoveryPort(consulProc.getHttpPort()).build();
	}

	private int getFreePort() {
		try (ServerSocket socket = new ServerSocket(0)) {
			return socket.getLocalPort();
		} catch (IOException e) {
			fail("Unable to get grpc port.");
		}
		throw new IllegalStateException("Error getting free port for gRPC server.");
	}

	private void registerServiceInConsul() {
		consul.agentClient().register(
			ImmutableRegistration.builder()
				.id("clarify-control-0")
				.address("localhost")
				.port(grpcServer.getPort())
				.name("clarify-control")
				.addTags("grpc")
				.build());
	}

}
