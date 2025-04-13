package server;

import utils.*;

import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

public class Server_JDXTEMM {
    private Map<BigInteger, byte[]> tset;
    private ArrayList<Bloom> f_array;//xset of join tables
    private ArrayList<Map<BigInteger, ArrayList<byte[]>>> cset_array;
    private int delta = 11000;
    private GGMTree ggmTree;

    public Server_JDXTEMM(Map<BigInteger, byte[]> tset, ArrayList<Bloom> f_array, ArrayList<Map<BigInteger, ArrayList<byte[]>>> cset_array) {
        this.tset = tset;
        this.f_array = f_array;
        this.cset_array = cset_array;
        this.ggmTree = new GGMTree(delta);
    }

    public ArrayList<ArrayList<ArrayList<byte[]>>> search(ArrayList<byte[]> stokenList, GGMXTokenList ggmxTokenList) throws NoSuchAlgorithmException, InvalidKeyException {
        ArrayList<ArrayList<ArrayList<byte[]>>> result = new ArrayList<>();
        int w_cnt_size = ggmxTokenList.w_cnt_List.length;
        byte[][] w_cnt_List = new byte[w_cnt_size - 1][];
//        for (int i = 1; i < w_cnt_size; i++) {
//            byte[] w_cnt = new byte[Cryptoutils.AES_BLOCK_SIZE];
//            System.arraycopy(ggmxTokenList.w_cnt_List[i].key, 0, w_cnt, 0, Cryptoutils.AES_BLOCK_SIZE);
//            ggmTree.derive_key_from_tree(w_cnt, i, ggmxTokenList.w_cnt_List[i].level, ggmTree.getLevel());
//            w_cnt_List[i - 1] = w_cnt;
//        }
        IntStream.range(1, w_cnt_size).parallel().forEach(i -> {
            byte[] w_cnt = new byte[Cryptoutils.AES_BLOCK_SIZE];
            System.arraycopy(ggmxTokenList.w_cnt_List[i].key, 0, w_cnt, 0, Cryptoutils.AES_BLOCK_SIZE);
            try {
                ggmTree.derive_key_from_tree(w_cnt, i, ggmxTokenList.w_cnt_List[i].level, ggmTree.getLevel());
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            } catch (InvalidKeyException e) {
                throw new RuntimeException(e);
            }
            w_cnt_List[i - 1] = w_cnt;
        });

        for (int i = 1; i <= stokenList.size(); i++) {
            byte[] alpha_i = tool.Xor(tset.get(new BigInteger(stokenList.get(i - 1))), w_cnt_List[i - 1]);
            ArrayList<byte[]> xtag_List = new ArrayList<>();
            ArrayList<ArrayList<byte[]>> result_i = new ArrayList<>();
            for (int j = 1; j <= f_array.size(); j++) {
                byte[] xtag = tool.Xor(alpha_i, ggmxTokenList.attrList.get(j - 1));
                if (!f_array.get(j - 1).mayContain(tool.bytesToLong(xtag)))
                    break;
                xtag_List.add(xtag);
                if (j == f_array.size()) {
                    for (int m = 0; m < xtag_List.size(); m++)
                        result_i.add(cset_array.get(m).get(new BigInteger(xtag_List.get(m))));
                }
            }
            if (!result_i.isEmpty())
                result.add(result_i);
        }
        return result;
    }
}
