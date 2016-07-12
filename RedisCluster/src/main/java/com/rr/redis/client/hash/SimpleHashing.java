package com.rr.redis.client.hash;

import java.util.Arrays;

/**
 * Simplest hash function. Use the hash in java itself.
 * 
 */
public class SimpleHashing implements IHashFunc {
	private final int hashSlotNum;

	public SimpleHashing(int hashNum) {
		this.hashSlotNum = hashNum;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.renren.ad.jedis.util.Hashing#hash(java.lang.String)
	 */
	@Override
	public int hash(String key) {
		return Math.abs(key.hashCode() % hashSlotNum);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.renren.ad.jedis.util.Hashing#hash(byte[])
	 */
	@Override
	public int hash(byte[] key) {
		return Math.abs(Arrays.hashCode(key) % hashSlotNum);
	}

	public int getHashSlotNum() {
		return hashSlotNum;
	}

}
