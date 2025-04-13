package server;

import utils.*;

import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Server_JDXT {
    private Map<BigInteger, byte[]> tset;
    private ArrayList<Bloom> f_array;//xset of join tables
    private ArrayList<Map<BigInteger, byte[]>> cset_array;
    private int delta = 11000;
    private GGMTree ggmTree;

    public Server_JDXT(Map<BigInteger, byte[]> tset, ArrayList<Bloom> f_array, ArrayList<Map<BigInteger, byte[]>> cset_array) {
        this.tset = tset;
        this.f_array = f_array;
        this.cset_array = cset_array;
        this.ggmTree = new GGMTree(delta);
    }

    public ArrayList<ArrayList<ArrayList<byte[]>>> search(ArrayList<byte[]> stokenList, GGMXTokenList ggmxTokenList) throws NoSuchAlgorithmException, InvalidKeyException {
        int w_cnt_size = ggmxTokenList.w_cnt_List.length;
        byte[][] w_cnt_List = new byte[w_cnt_size - 1][];
        ArrayList<byte[][]> w_jk_List = new ArrayList<>();
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

//        for (int j = 1; j <= f_array.size(); j++) {
//            int k_size = ggmxTokenList.w_jk_List.get(j - 1).length;
//            byte[][] tmp_List = new byte[k_size - 1][];
//            for (int k = 1; k < k_size; k++) {
//                byte[] w_jk = new byte[Cryptoutils.AES_BLOCK_SIZE];
//                System.arraycopy(ggmxTokenList.w_jk_List.get(j - 1)[k].key, 0, w_jk, 0, Cryptoutils.AES_BLOCK_SIZE);
//                ggmTree.derive_key_from_tree(w_jk, k, ggmxTokenList.w_jk_List.get(j - 1)[k].level, ggmTree.getLevel());
//                tmp_List[k - 1] = tool.Xor(ggmxTokenList.attrList.get(j - 1), w_jk);
//            }
//            w_jk_List.add(tmp_List);
//        }

        Map<Integer, byte[][]> tempMap = new ConcurrentHashMap<>();
        IntStream.range(0, f_array.size()).parallel().forEach(j -> {
            int k_size = ggmxTokenList.w_jk_List.get(j).length;
            byte[][] tmp_List = new byte[k_size - 1][];
            byte[] attr_bytes = ggmxTokenList.attrList.get(j);
            IntStream.range(1, k_size).forEach(k -> {
                byte[] w_jk = new byte[Cryptoutils.AES_BLOCK_SIZE];
                System.arraycopy(ggmxTokenList.w_jk_List.get(j)[k].key, 0, w_jk, 0, Cryptoutils.AES_BLOCK_SIZE);
                try {
                    ggmTree.derive_key_from_tree(w_jk, k, ggmxTokenList.w_jk_List.get(j)[k].level, ggmTree.getLevel());
                } catch (NoSuchAlgorithmException | InvalidKeyException e) {
                    throw new RuntimeException(e);
                }
                tmp_List[k - 1] = tool.Xor(attr_bytes, w_jk);
            });
            tempMap.put(j, tmp_List);
        });
        for (int j = 0; j < f_array.size(); j++)
            w_jk_List.add(tempMap.get(j));


        ArrayList<ArrayList<ArrayList<byte[]>>> result = new ArrayList<>();
        for (int i = 1; i <= stokenList.size(); i++) {
            ArrayList<ArrayList<byte[]>> xtagArray_i = new ArrayList<>();
            ArrayList<ArrayList<byte[]>> result_i = new ArrayList<>();
            byte[] alpha_i = tool.Xor(tset.get(new BigInteger(stokenList.get(i - 1))), w_cnt_List[i - 1]);
            for (int j = 1; j <= f_array.size(); j++) {
                int finalJ = j;
                ArrayList<byte[]> xtagArray_ij = IntStream.range(0, w_jk_List.get(j - 1).length)
                        .parallel()
                        .mapToObj(k -> {
                            byte[] xtag_ijk = tool.Xor(alpha_i, w_jk_List.get(finalJ - 1)[k]);
                            return f_array.get(finalJ - 1).mayContain(tool.bytesToLong(xtag_ijk)) ? xtag_ijk : null;
                        })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toCollection(ArrayList::new));
//                ArrayList<byte[]> xtagArray_ij = new ArrayList<>();//xtag_ij
//
//                for (int k = 1; k <= w_jk_List.get(j - 1).length; k++) {
//                    byte[] xtag_ijk = tool.Xor(alpha_i, w_jk_List.get(j - 1)[k - 1]);
//                    if (f_array.get(j - 1).mayContain(tool.bytesToLong(xtag_ijk)))
//                        xtagArray_ij.add(xtag_ijk);
//                }
                if (xtagArray_ij.isEmpty())
                    break;
                xtagArray_i.add(xtagArray_ij);
                if (j == f_array.size()) {
//                    for (int m = 0; m < xtagArray_i.size(); m++) {
//                        ArrayList<byte[]> result_ij = new ArrayList<>();
//                        for (int n = 0; n < xtagArray_i.get(m).size(); n++)
//                            result_ij.add(cset_array.get(m).get(new BigInteger(xtagArray_i.get(m).get(n))));
//                        result_i.add(result_ij);
//                    }
                    for (int m = 0; m < xtagArray_i.size(); m++) {
                        ArrayList<byte[]> result_ij;
                        int finalM = m;
                        result_ij = IntStream.range(0, xtagArray_i.get(finalM).size())
                                .parallel()
                                .mapToObj(n -> cset_array.get(finalM).get(new BigInteger(xtagArray_i.get(finalM).get(n))))
                                .collect(Collectors.toCollection(ArrayList::new));
                        result_i.add(result_ij);
                    }
                }
            }
            if (!result_i.isEmpty())
                result.add(result_i);
        }
        return result;
    }
}
