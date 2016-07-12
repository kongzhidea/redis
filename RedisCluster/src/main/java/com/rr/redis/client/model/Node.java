package com.rr.redis.client.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class Node {
	private String host;
	private int port;

	public Node() {
	}

	public Node(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	@Override
	public boolean equals(Object o) {
		if (o == null) {
			return false;
		}
		if (!(o instanceof Node)) {
			return false;
		}
		Node otherNode = (Node) o;
		if (StringUtils.equals(host, otherNode.getHost())
				&& port == otherNode.getPort()) {
			return true;
		}
		return false;
	}

	public int hashCode() {
		return (host + ":" + port).hashCode();
	}

	public String toString() {
		return host + ":" + port;
	}

	public String getIdentity() {
		return host + ":" + port;
	}

	public static Node getNodeFromIdentity(String identity) {
		try {
			String[] conts = StringUtils.split(identity, ":");
			String host = conts[0];
			String port = conts[1];
			Node node = new Node(host, Integer.valueOf(port));
			return node;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

}