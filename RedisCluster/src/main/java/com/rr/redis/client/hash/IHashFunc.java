package com.rr.redis.client.hash;

/**
 * Interface for hash function.
 * 
 */
public interface IHashFunc {
	public int hash(String key);

	public int hash(byte[] key);
	
	public int getHashSlotNum();
}
