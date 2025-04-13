package utils;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

public class Cryptoutils {
    public static int AES_BLOCK_SIZE = 32;

    private static final ThreadLocal<Mac> hmacSHA256Instance = ThreadLocal.withInitial(() -> {
        try {
            return Mac.getInstance("HmacSHA256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    });

    public static byte[] keyDerivation(byte[] plaintext, byte[] key) {
        try {
            Mac mac = hmacSHA256Instance.get();
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(plaintext);
        } catch (InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }


    public static GGMNode[] compute_PRFs(GGMTree ggmTree, byte[] root_key, int num_values) throws NoSuchAlgorithmException, InvalidKeyException {
        ArrayList<GGMNode> node_list = new ArrayList<>();
        for (int i = 0; i < num_values; i++)
            node_list.add(new GGMNode(i, ggmTree.getLevel()));
        ArrayList<GGMNode> coverage_nodes = ggmTree.min_coverage(node_list);
        GGMNode[] prf_values = new GGMNode[num_values];

        for (GGMNode ggmNode : coverage_nodes) {
            byte[] derived_key = new byte[Cryptoutils.AES_BLOCK_SIZE];
            System.arraycopy(root_key, 0, derived_key, 0, Cryptoutils.AES_BLOCK_SIZE);
            ggmTree.derive_key_from_tree(derived_key, ggmNode.index, 0, ggmNode.level);
            System.arraycopy(derived_key, 0, ggmNode.key, 0, Cryptoutils.AES_BLOCK_SIZE);
            for (int i = 0; i < num_values; i++) {
                if ((i >> (ggmTree.getLevel() - ggmNode.level)) == ggmNode.index)
                    prf_values[i] = ggmNode;
            }
        }
        return prf_values;
    }
}



