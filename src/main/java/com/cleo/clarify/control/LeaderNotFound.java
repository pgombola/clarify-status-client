package com.cleo.clarify.control;

public class LeaderNotFound extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public LeaderNotFound(String serviceName) {
		super(msg(serviceName));
	}

	public LeaderNotFound(String serviceName, Throwable cause) {
		super(msg(serviceName), cause);
	}
	
	private static String msg(String serviceName) {
		return String.format("{\"serviceName\": \"%s\"}", serviceName);
	}
}
