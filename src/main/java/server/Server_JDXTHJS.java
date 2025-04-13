package server;

import utils.*;

import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Server_JDXTHJS {
    private Map<BigInteger, byte[]> tset;
    private ArrayList<Integer> table_EBTList;
    private int delta = 11000;
    private GGMTree ggmTree;

    public Server_JDXTHJS(Map<BigInteger, byte[]> tset, ArrayList<Integer> table_EBTList) {
        this.tset = tset;
        this.table_EBTList = table_EBTList;
        this.ggmTree = new GGMTree(delta);
    }



    public Restuple search(ArrayList<byte[]> stokenList, GGMXTokenList ggmxTokenList) throws NoSuchAlgorithmException, InvalidKeyException {
        ArrayList<ArrayList<ArrayList<byte[]>>> xtag_array = new ArrayList<>();
        ArrayList<ArrayList<ArrayList<int[]>>> result = new ArrayList<>();
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

//        for (int j = 1; j <= table_EBTList.size(); j++) {
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
        IntStream.range(0, table_EBTList.size()).parallel().forEach(j -> {
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
        for (int j = 0; j < table_EBTList.size(); j++)
            w_jk_List.add(tempMap.get(j));


//        for (int i = 1; i <= stokenList.size(); i++) {
//            byte[] alpha_i = tool.Xor(tset.get(new BigInteger(stokenList.get(i - 1))), w_cnt_List[i - 1]);
//            ArrayList<ArrayList<int[]>> result_i = new ArrayList<>();
//            ArrayList<ArrayList<byte[]>> xtag_i = new ArrayList<>();
//            for (int j = 1; j <= table_EBTList.size(); j++) {
//                ArrayList<int[]> result_j = new ArrayList<>();
//                ArrayList<byte[]> xtag_j = new ArrayList<>();
//                for (int k = 1; k <= w_jk_List.get(j - 1).length; k++) {
//                    byte[] xtag_ijk = tool.Xor(alpha_i, w_jk_List.get(j - 1)[k - 1]);
//                    xtag_j.add(xtag_ijk);
//                    if (table_EBTList.get(j - 1) != 0) {
//                        int[] ct_index = HJS.HJSRespond(xtag_ijk);
//                        result_j.add(ct_index);
//                    }
//                }
//                xtag_i.add(xtag_j);
//                result_i.add(result_j);
//            }
//            xtag_array.add(xtag_i);
//            result.add(result_i);
//        }

        int numThreads = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<ArrayList<ArrayList<byte[]>>>> futures = new ArrayList<>();
        for (int i = 1; i <= stokenList.size(); i++) {
            final int idx = i;
            futures.add(executor.submit(() -> {
                byte[] alpha_i = tool.Xor(tset.get(new BigInteger(stokenList.get(idx - 1))), w_cnt_List[idx - 1]);
                ArrayList<ArrayList<byte[]>> xtag_i = new ArrayList<>();
                for (int j = 1; j <= table_EBTList.size(); j++) {
                    int finalJ = j;
                    ArrayList<byte[]> xtag_j = IntStream.range(0, w_jk_List.get(finalJ - 1).length)
                            .parallel()
                            .mapToObj(k -> tool.Xor(alpha_i, w_jk_List.get(finalJ - 1)[k]))
                            .collect(Collectors.toCollection(ArrayList::new));

                    xtag_i.add(xtag_j);
                }
                return xtag_i;
            }));
        }

        for (Future<ArrayList<ArrayList<byte[]>>> future : futures) {
            try {
                xtag_array.add(future.get());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        executor.shutdown();

        int numThreads_ = Runtime.getRuntime().availableProcessors();
        ExecutorService executor_ = Executors.newFixedThreadPool(numThreads_);
        List<Future<ArrayList<ArrayList<int[]>>>> futures_ = new ArrayList<>();
        for (int i = 1; i <= stokenList.size(); i++) {
            final int idx = i;
            futures_.add(executor_.submit(() -> {
                ArrayList<ArrayList<int[]>> result_i = new ArrayList<>();
                for (int j = 1; j <= table_EBTList.size(); j++) {
                    ArrayList<int[]> result_j = new ArrayList<>();

                    for (int k = 1; k <= w_jk_List.get(j - 1).length; k++) {
                        byte[] xtag_ijk = xtag_array.get(idx - 1).get(j - 1).get(k - 1);
                        if (table_EBTList.get(j - 1) != 0) {
                            int[] ct_index = HJS.HJSRespond(xtag_ijk);
                            result_j.add(ct_index);
                        }
                    }
                    result_i.add(result_j);
                }
                return result_i;
            }));
        }
        for (Future<ArrayList<ArrayList<int[]>>> future : futures_) {
            try {
                result.add(future.get());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        executor_.shutdown();

        return new Restuple(result, xtag_array);
    }
}
