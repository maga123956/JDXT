package utils;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

public class GGMTree {
    private int level;

    public GGMTree(long num_node) {
        this.level = (int) Math.ceil(Math.log(num_node) / Math.log(2));
    }
    public void derive_key_from_tree(byte[] current_key, long offset, int start_level, int target_level) throws NoSuchAlgorithmException, InvalidKeyException {
        if (start_level == target_level) {
            return;
        }
        if (current_key == null || current_key.length != Cryptoutils.AES_BLOCK_SIZE) {
            throw new IllegalArgumentException("Invalid currentKey");
        }
        for (int k = target_level - start_level; k >= 1; --k) {
            int k_bit = (int) ((offset & (1L << (k - 1))) >>> (k - 1));
            byte[] k_bytes = tool.intToBytes(k_bit);
            byte[] nextKey = Cryptoutils.keyDerivation(k_bytes, current_key);
            System.arraycopy(nextKey, 0, current_key, 0, Cryptoutils.AES_BLOCK_SIZE);
        }
    }

    public ArrayList<GGMNode> min_coverage(ArrayList<GGMNode> node_list) {
        ArrayList<GGMNode> next_level_node = new ArrayList<>();
        for (int i = 0; i < node_list.size(); ++i) {
            GGMNode node1 = node_list.get(i);
            if (i + 1 == node_list.size())
                next_level_node.add(node1);
            else {
                GGMNode node2 = node_list.get(i + 1);
                if ((node1.index >> 1) == (node2.index >> 1) && (node1.level == node2.level)) {
                    next_level_node.add(new GGMNode(node1.index >> 1, node1.level - 1));
                    i++;
                } else {
                    next_level_node.add(node1);
                }
            }
        }

        if (next_level_node.size() == node_list.size() || next_level_node.isEmpty())
            return node_list;
        return min_coverage(next_level_node);
    }

    public int getLevel() {
        return level;
    }
}
