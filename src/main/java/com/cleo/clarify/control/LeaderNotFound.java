package com.cleo.clarify.control;

public class LeaderNotFound extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public LeaderNotFound(String serviceName) {
		super(String.format("{serviceName=%s}", serviceName));
	}
}
