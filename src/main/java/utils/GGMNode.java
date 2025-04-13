package utils;

public class GGMNode {
    public long index;
    public int level;
    public byte[] key;
    GGMNode(long index, int level){
        this.index = index;
        this.level = level;
        key = new byte[Cryptoutils.AES_BLOCK_SIZE];
    }
    GGMNode(long index, int level, byte[] key){
        this.index = index;
        this.level = level;
        this.key = new byte[Cryptoutils.AES_BLOCK_SIZE];
        System.arraycopy(key, 0, this.key, 0, Cryptoutils.AES_BLOCK_SIZE);
    }
}
