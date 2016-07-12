package com.rr.redis.client.exception;

public class ClusterOpException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public ClusterOpException(String msg, Throwable e) {
		super(msg, e);
	}

	public ClusterOpException(String msg) {
		super(msg);
	}

	public ClusterOpException(Throwable e) {
		super(e);
	}
}