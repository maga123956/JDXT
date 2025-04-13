package utils;

import client.Setup_JDXT;
import client.Setup_JDXTEMM;
import client.Setup_JDXTHJS;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class query {
    private static String K_r = "8975924566f6e252";
    private static String K_x = "89b7a92966f6eb32";
    private static String K_z = "9862192ad6f6ef65";
    private static String K_z1 = "9874a22554e7db85";
    private static String K_f = "6574b33984e7fb55";
    private static String K_c = "787599ac86f2e82";
    private static byte[] K_f_bytes;

    private String ind;
    private String w;
    private String op;
    private ArrayList<Jointuple> joinArray;

    private ArrayList<Object> table_array;
    private ArrayList<String> val_array;
    private ArrayList<String> attr_array;

    private ArrayList<byte[]> stokenList;
    private GGMXTokenList ggmxTokenList;
    private int prior_pos;

    private GGMTree ggmTree;
    private int delta = 11000; //规定一个GGM Tree生成上限

    public query(String ind_, String w_, String op_, ArrayList<Jointuple> joinArray_) {
        ind = ind_;
        w = w_;
        op = op_;
        joinArray = joinArray_;
    }

    public query(ArrayList<Object> table_array, ArrayList<String> val_array, ArrayList<String> attr_array) {
        this.table_array = table_array;
        this.val_array = val_array;
        this.attr_array = attr_array;
        stokenList = new ArrayList<>();
        ggmTree = new GGMTree(delta);
        K_f_bytes = K_f.getBytes(StandardCharsets.UTF_8);
    }

    public void JDXT_getJoinToken() throws NoSuchAlgorithmException, InvalidKeyException {
        prior_pos = 0;
        int mincnt;
        if (!((Setup_JDXT) table_array.get(0)).getC().containsKey(val_array.get(0))) {
            System.out.println("in getJoinToken, table doesn't have keyword");
            return;
        } else
            mincnt = ((Setup_JDXT) table_array.get(0)).getC().get(val_array.get(0));
        for (int i = 1; i < table_array.size(); i++) {
            int tmp;
            if (!((Setup_JDXT) table_array.get(i)).getC().containsKey(val_array.get(i))) {
                System.out.println("in getJoinToken, table doesn't have keyword");
                return;
            } else
                tmp = ((Setup_JDXT) table_array.get(i)).getC().get(val_array.get(i));
            if (mincnt > tmp) {
                prior_pos = i;
                mincnt = tmp;
            }
        }
        String attr_t = attr_array.get(prior_pos);
        String w_t = val_array.get(prior_pos);

        byte[] w_cnt_rootKey = Hash.Get_SHA_256((K_z + w_t).getBytes(StandardCharsets.UTF_8));
        ArrayList<byte[]> attrList = new ArrayList<>();
        GGMNode[] w_cnt_List = Cryptoutils.compute_PRFs(ggmTree, w_cnt_rootKey, mincnt + 1);
        ArrayList<GGMNode[]> w_jk_List = new ArrayList<>();

        for (int j = 1; j <= table_array.size(); j++) {
            byte[] w1_cnt_rootKey = Hash.Get_SHA_256((K_z1 + val_array.get(j - 1)).getBytes(StandardCharsets.UTF_8));
            int Cj_size = ((Setup_JDXT) table_array.get(j - 1)).getC().get(val_array.get(j - 1));
            GGMNode[] node_list = Cryptoutils.compute_PRFs(ggmTree, w1_cnt_rootKey, Cj_size + 1);
            w_jk_List.add(node_list);
            attrList.add(Hash.Get_SHA_256((K_x + attr_array.get(j - 1)).getBytes(StandardCharsets.UTF_8)));
        }
        ggmxTokenList = new GGMXTokenList(attrList, w_cnt_List, w_jk_List);

        for (int i = 1; i <= mincnt; i++) {
            byte[] saddr = Hash.Get_SHA_256((K_r + w_t + i + attr_t).getBytes(StandardCharsets.UTF_8));
            stokenList.add(saddr);
        }
    }

    public void JDXTHJS_getJoinToken() throws NoSuchAlgorithmException, InvalidKeyException {
        prior_pos = 0;
        int mincnt;
        if (!((Setup_JDXTHJS) table_array.get(0)).getC().containsKey(val_array.get(0))) {
            System.out.println("in getJoinToken, table doesn't have keyword");
            return;
        } else
            mincnt = ((Setup_JDXTHJS) table_array.get(0)).getC().get(val_array.get(0));
        for (int i = 1; i < table_array.size(); i++) {
            int tmp;
            if (!((Setup_JDXTHJS) table_array.get(i)).getC().containsKey(val_array.get(i))) {
                System.out.println("in getJoinToken, table doesn't have keyword");
                return;
            } else
                tmp = ((Setup_JDXTHJS) table_array.get(i)).getC().get(val_array.get(i));
            if (mincnt > tmp) {
                prior_pos = i;
                mincnt = tmp;
            }
        }
        String attr_t = attr_array.get(prior_pos);
        String w_t = val_array.get(prior_pos);
        byte[] w_cnt_rootKey = Hash.Get_SHA_256((K_z + w_t).getBytes(StandardCharsets.UTF_8));
        ArrayList<byte[]> attrList = new ArrayList<>();
        GGMNode[] w_cnt_List = Cryptoutils.compute_PRFs(ggmTree, w_cnt_rootKey, mincnt + 1);
        ArrayList<GGMNode[]> w_jk_List = new ArrayList<>();
        for (int j = 1; j <= table_array.size(); j++) {
            byte[] w1_cnt_rootKey = Hash.Get_SHA_256((K_z1 + val_array.get(j - 1)).getBytes(StandardCharsets.UTF_8));
            int Cj_size = ((Setup_JDXTHJS) table_array.get(j - 1)).getC().get(val_array.get(j - 1));
            GGMNode[] node_list = Cryptoutils.compute_PRFs(ggmTree, w1_cnt_rootKey, Cj_size + 1);
            w_jk_List.add(node_list);
            attrList.add(Hash.Get_SHA_256((K_x + attr_array.get(j - 1)).getBytes(StandardCharsets.UTF_8)));
        }
        ggmxTokenList = new GGMXTokenList(attrList, w_cnt_List, w_jk_List);
        for (int i = 1; i <= mincnt; i++) {
            byte[] saddr = Hash.Get_SHA_256((K_r + w_t + i + attr_t).getBytes(StandardCharsets.UTF_8));
            stokenList.add(saddr);
        }
    }

    public void JDXTEMM_getJoinToken() throws NoSuchAlgorithmException, InvalidKeyException {
        prior_pos = 0;
        int mincnt;
        if (!((Setup_JDXTEMM) table_array.get(0)).getC().containsKey(val_array.get(0))) {
            System.out.println("in getJoinToken, table doesn't have keyword");
            return;
        } else
            mincnt = ((Setup_JDXTEMM) table_array.get(0)).getC().get(val_array.get(0));
        for (int i = 1; i < table_array.size(); i++) {
            int tmp;
            if (!((Setup_JDXTEMM) table_array.get(i)).getC().containsKey(val_array.get(i))) {
                System.out.println("in getJoinToken, table doesn't have keyword");
                return;
            } else
                tmp = ((Setup_JDXTEMM) table_array.get(i)).getC().get(val_array.get(i));
            if (mincnt > tmp) {
                prior_pos = i;
                mincnt = tmp;
            }
        }
        String attr_t = attr_array.get(prior_pos);
        String w_t = val_array.get(prior_pos);
        byte[] w_cnt_rootKey = Hash.Get_SHA_256((K_z + w_t).getBytes(StandardCharsets.UTF_8));
        ArrayList<byte[]> attrList = new ArrayList<>();//F(K_z1,w_j)+F(K_x,attr_t)+F(K_c+1)
        GGMNode[] w_cnt_List = Cryptoutils.compute_PRFs(ggmTree, w_cnt_rootKey, mincnt + 1);
        byte[] prf = Hash.Get_SHA_256((K_c + 1).getBytes(StandardCharsets.UTF_8));
        for (int j = 1; j <= table_array.size(); j++)
            attrList.add(tool.Xor(prf, tool.Xor(Objects.requireNonNull(Hash.Get_SHA_256((K_z1 + val_array.get(j - 1)).getBytes(StandardCharsets.UTF_8))), Objects.requireNonNull(Hash.Get_SHA_256((K_x + attr_array.get(j - 1)).getBytes(StandardCharsets.UTF_8))))));
        ggmxTokenList = new GGMXTokenList(attrList, w_cnt_List);
        for (int i = 1; i <= mincnt; i++) {
            byte[] saddr = Hash.Get_SHA_256((K_r + w_t + i + attr_t).getBytes(StandardCharsets.UTF_8));
            stokenList.add(saddr);
        }
    }

    public void JDXT_filter_ind(ArrayList<byte[]> encRe, byte[] decrypt_key, Map<String, Integer> filter_tab, ArrayList<String> pos_tab) {
        for (byte[] val : encRe) {
            byte[] record_byte = tool.Xor(decrypt_key, val);
            String record = new String(record_byte);
            String[] ind_op = record.split("\\|");
            if (!filter_tab.containsKey(ind_op[0])) {
                filter_tab.put(ind_op[0], 1);
                pos_tab.add(ind_op[0]);
            } else {
                int cnt = filter_tab.get(ind_op[0]);
                if (ind_op[1].equals("add"))
                    cnt++;
                else
                    cnt--;
                filter_tab.put(ind_op[0], cnt);
            }
        }
    }

    public ArrayList<ArrayList<ArrayList<String>>> JDXT_filter_res(ArrayList<ArrayList<ArrayList<byte[]>>> enc_res) {
        ArrayList<ArrayList<ArrayList<String>>> res = new ArrayList<>();
        int table_num = table_array.size();
        ArrayList<byte[]> keyList = new ArrayList<>();
        for (int i = 0; i < table_num; i++)
            keyList.add(Hash.Get_SHA_256((K_r + val_array.get(i) + attr_array.get(i)).getBytes(StandardCharsets.UTF_8)));

        ArrayList<Integer> deletePos = new ArrayList<>();
        for (int i = 0; i < enc_res.size(); i++) {
            ArrayList<byte[]> prior_encList = enc_res.get(i).get(prior_pos);
            Map<String, Integer> filter_prior = new HashMap<>();
            ArrayList<String> pos_prior = new ArrayList<>();
            for (byte[] val : prior_encList) {
                byte[] record_byte = tool.Xor(keyList.get(prior_pos), val);
                String record = new String(record_byte);
                String[] ind_op = record.split("\\|");
                if (!filter_prior.containsKey(ind_op[0])) {
                    filter_prior.put(ind_op[0], 1);
                    pos_prior.add(ind_op[0]);
                } else {
                    int cnt = filter_prior.get(ind_op[0]);
                    if (ind_op[1].equals("add"))
                        cnt++;
                    else
                        cnt--;
                    filter_prior.put(ind_op[0], cnt);
                }
            }
            int flag = 1;
            for (String str : pos_prior) {
                if (filter_prior.get(str) >= 1) {
                    flag = 0;
                    break;
                }
            }
            if (flag != 0)
                deletePos.add(i);
        }

        for (int i = 0; i < enc_res.size(); i++) {
            if (deletePos.contains(i))
                continue;
            ArrayList<ArrayList<String>> res_i = new ArrayList<>();
            for (int j = 0; j < table_num; j++) {
                ArrayList<String> res_ij = new ArrayList<>();
                ArrayList<byte[]> encRe = enc_res.get(i).get(j);
                Map<String, Integer> filter_tab = new HashMap<>();
                ArrayList<String> pos_tab = new ArrayList<>();
                JDXT_filter_ind(encRe, keyList.get(j), filter_tab, pos_tab);
                for (String s : pos_tab) {
                    if (filter_tab.get(s) < 1)
                        continue;
                    res_ij.add(s);
                }
                if (!res_ij.isEmpty())
                    res_i.add(res_ij);
                else {
                    res_i.clear();
                    break;
                }
            }
            if (!res_i.isEmpty())
                res.add(res_i);
        }
        return res;
    }

    public ArrayList<ArrayList<ArrayList<String>>> JDXT_get_res(ArrayList<ArrayList<ArrayList<byte[]>>> enc_res) {
        int table_num = table_array.size();
        ArrayList<byte[]> keyList = new ArrayList<>();
        for (int i = 0; i < table_num; i++)
            keyList.add(Hash.Get_SHA_256((K_r + val_array.get(i) + attr_array.get(i)).getBytes(StandardCharsets.UTF_8)));
        ArrayList<ArrayList<ArrayList<String>>> res = new ArrayList<>();
        for (int i = 0; i < enc_res.size(); i++) {
            ArrayList<ArrayList<String>> res_i = new ArrayList<>();
            for (int j = 0; j < table_num; j++) {
                ArrayList<byte[]> encRe = enc_res.get(i).get(j);
                TreeSet<String> indSet = new TreeSet<>();
                for (byte[] val : encRe) {
                    if (val == null)
                        continue;
                    byte[] record_byte = tool.Xor(keyList.get(j), val);
                    String record = new String(record_byte);
                    String[] ind_op = record.split("\\|");
                    indSet.add(ind_op[0]);
                }
                ArrayList<String> res_ij = new ArrayList<>(indSet);
                res_i.add(res_ij);
            }
            res.add(res_i);
        }
        return res;
    }


    public void JDXTHJS_filter_ind(ArrayList<byte[]> encRe, byte[] decrypt_key, Map<String, Integer> filter_tab, ArrayList<String> pos_tab) {
        for (byte[] val : encRe) {
            byte[] record_byte = tool.Xor(decrypt_key, val);
            String record = new String(record_byte);
            String[] ind_op = record.split("\\|");
            if (!filter_tab.containsKey(ind_op[0])) {
                filter_tab.put(ind_op[0], 1);
                pos_tab.add(ind_op[0]);
            } else {
                int cnt = filter_tab.get(ind_op[0]);
                if (ind_op[1].equals("add"))
                    cnt++;
                else
                    cnt--;
                filter_tab.put(ind_op[0], cnt);
            }
        }
    }

    public ArrayList<ArrayList<ArrayList<String>>> JDXTHJS_filter_res(ArrayList<ArrayList<ArrayList<int[]>>> enc_res, ArrayList<ArrayList<ArrayList<byte[]>>> xtag_array) {
        ArrayList<ArrayList<ArrayList<String>>> res = new ArrayList<>();
        ArrayList<byte[]> K_f_cnt_List = new ArrayList<>();
        ArrayList<byte[]> Key_List = new ArrayList<>();
        int table_num = table_array.size();
        int maxCnt = 0;
        for (int i = 0; i < table_num; i++) {
            maxCnt = Math.max(((Setup_JDXTHJS) table_array.get(i)).getCnt(), maxCnt);
            Key_List.add(Hash.Get_SHA_256((K_r + val_array.get(i) + attr_array.get(i)).getBytes(StandardCharsets.UTF_8)));
        }

        long K_f_tmp = tool.bytesToLong(K_f_bytes);
        for (int i = 0; i < maxCnt; i++) {
            K_f_tmp = Hash.hash64(K_f_tmp, HJS.seed);
            K_f_cnt_List.add(tool.longToBytes(K_f_tmp));
        }

        ArrayList<Integer> deletePos = new ArrayList<>();
        for (int i = 0; i < xtag_array.size(); i++) {
            ArrayList<byte[]> val_iprior = new ArrayList<>();
            int k_len = xtag_array.get(i).get(prior_pos).size();
            for (int k = 0; k < k_len; k++) {
                byte[] xtag = xtag_array.get(i).get(prior_pos).get(k);
                if (!enc_res.get(i).get(prior_pos).isEmpty()) {
                    int[] ct_indexList = enc_res.get(i).get(prior_pos).get(k);
                    int Cnt = ((Setup_JDXTHJS) table_array.get(prior_pos)).getCnt();
                    for (int m = 0; m < Cnt; m++) {
                        byte[] tag = tool.Xor(xtag, K_f_cnt_List.get(m));
                        for (int id : ct_indexList) {
                            Map<BigInteger, byte[]> ebt_bucket = ((Setup_JDXTHJS) table_array.get(prior_pos)).getEBT_array().get(m).get(id);
                            byte[] val = HJS.HJSTest(tag, ebt_bucket);
                            if (val != null) {
                                val_iprior.add(val);
                                break;
                            }
                        }
                    }
                }
                Map<BigInteger, byte[]> xset = ((Setup_JDXTHJS) table_array.get(prior_pos)).getXset();
                BigInteger xtag_integer = new BigInteger(xtag);
                if (xset.containsKey(xtag_integer))
                    val_iprior.add(xset.get(xtag_integer));
            }
            Map<String, Integer> filter_prior = new HashMap<>();
            ArrayList<String> pos_prior = new ArrayList<>();
            for (byte[] val : val_iprior) {
                byte[] record_byte = tool.Xor(Key_List.get(prior_pos), val);
                String record = new String(record_byte);
                String[] ind_op = record.split("\\|");
                if (!filter_prior.containsKey(ind_op[0])) {
                    filter_prior.put(ind_op[0], 1);
                    pos_prior.add(ind_op[0]);
                } else {
                    int cnt = filter_prior.get(ind_op[0]);
                    if (ind_op[1].equals("add"))
                        cnt++;
                    else
                        cnt--;
                    filter_prior.put(ind_op[0], cnt);
                }
            }
            int flag = 1;
            for (String str : pos_prior) {
                if (filter_prior.get(str) >= 1) {
                    flag = 0;
                    break;
                }
            }

            if (flag != 0)
                deletePos.add(i);
        }

        for (int i = 0; i < xtag_array.size(); i++) {
            if (deletePos.contains(i))
                continue;
            ArrayList<ArrayList<String>> res_i = new ArrayList<>();
            ArrayList<ArrayList<byte[]>> val_i = new ArrayList<>();
            for (int j = 0; j < table_num; j++) {
                ArrayList<byte[]> val_ij = new ArrayList<>();
                int k_len = xtag_array.get(i).get(j).size();
                for (int k = 0; k < k_len; k++) {
                    byte[] xtag = xtag_array.get(i).get(j).get(k);
                    if (!enc_res.get(i).get(j).isEmpty()) {
                        int[] ct_indexList = enc_res.get(i).get(j).get(k);
                        int Cnt = ((Setup_JDXTHJS) table_array.get(j)).getCnt();
                        for (int m = 0; m < Cnt; m++) {
                            byte[] tag = tool.Xor(xtag, K_f_cnt_List.get(m));
                            for (int id : ct_indexList) {
                                Map<BigInteger, byte[]> ebt_bucket = ((Setup_JDXTHJS) table_array.get(j)).getEBT_array().get(m).get(id);
                                byte[] val = HJS.HJSTest(tag, ebt_bucket);
                                if (val != null) {
                                    val_ij.add(val);
                                    break;
                                }
                            }
                        }
                    }
                    Map<BigInteger, byte[]> xset = ((Setup_JDXTHJS) table_array.get(j)).getXset();
                    BigInteger xtag_integer = new BigInteger(xtag);
                    if (xset.containsKey(xtag_integer))
                        val_ij.add(xset.get(xtag_integer));
                }
                if (val_ij.isEmpty()) {
                    break;
                } else
                    val_i.add(val_ij);

                if (j == table_num - 1) {
                    for (int m = 0; m < table_num; m++) {
                        ArrayList<String> res_ij = new ArrayList<>();
                        Map<String, Integer> filter_tab = new HashMap<>();
                        ArrayList<String> pos_tab = new ArrayList<>();
                        JDXTHJS_filter_ind(val_i.get(m), Key_List.get(m), filter_tab, pos_tab);
                        for (String s : pos_tab) {
                            if (filter_tab.get(s) < 1)
                                continue;
                            res_ij.add(s);
                        }
                        if (!res_ij.isEmpty())
                            res_i.add(res_ij);
                        else {
                            res_i.clear();
                            break;
                        }
                    }
                }
            }
            if (!res_i.isEmpty())
                res.add(res_i);
        }
        return res;
    }

    public ArrayList<ArrayList<ArrayList<String>>> JDXTHJS_filter_res_parallel(ArrayList<ArrayList<ArrayList<int[]>>> enc_res, ArrayList<ArrayList<ArrayList<byte[]>>> xtag_array) {
        ArrayList<ArrayList<ArrayList<String>>> res = new ArrayList<>();
        ArrayList<byte[]> K_f_cnt_List = new ArrayList<>();
        ArrayList<byte[]> Key_List = new ArrayList<>();
        int table_num = table_array.size();
        int maxCnt = 0;
        for (int i = 0; i < table_num; i++) {
            maxCnt = Math.max(((Setup_JDXTHJS) table_array.get(i)).getCnt(), maxCnt);
            Key_List.add(Hash.Get_SHA_256((K_r + val_array.get(i) + attr_array.get(i)).getBytes(StandardCharsets.UTF_8)));
        }

        long K_f_tmp = tool.bytesToLong(K_f_bytes);
        for (int i = 0; i < maxCnt; i++) {
            K_f_tmp = Hash.hash64(K_f_tmp, HJS.seed);
            K_f_cnt_List.add(tool.longToBytes(K_f_tmp));
        }

        for (int i = 0; i < xtag_array.size(); i++) {
            ArrayList<ArrayList<String>> res_i = new ArrayList<>();
            ArrayList<ArrayList<byte[]>> val_i = new ArrayList<>();
            for (int j = 0; j < table_num; j++) {
                int k_len = xtag_array.get(i).get(j).size();
                int finalI = i;
                int finalJ = j;
                List<byte[]> results = IntStream.range(0, k_len).parallel()
                        .mapToObj(k -> {
                            byte[] xtag = xtag_array.get(finalI).get(finalJ).get(k);
                            List<byte[]> foundValues = new ArrayList<>();
                            if (!enc_res.get(finalI).get(finalJ).isEmpty()) {
                                int[] ct_indexList = enc_res.get(finalI).get(finalJ).get(k);
                                int Cnt = ((Setup_JDXTHJS) table_array.get(finalJ)).getCnt();
                                IntStream.range(0, Cnt).parallel().forEach(m -> {
                                    byte[] tag = tool.Xor(xtag, K_f_cnt_List.get(m));

                                    for (int id : ct_indexList) {
                                        Map<BigInteger, byte[]> ebt_bucket = ((Setup_JDXTHJS) table_array.get(finalJ))
                                                .getEBT_array().get(m).get(id);
                                        byte[] val = HJS.HJSTest(tag, ebt_bucket);
                                        if (val != null) {
                                            synchronized (foundValues) {
                                                foundValues.add(val);
                                            }
                                            break;
                                        }
                                    }
                                });
                            }
                            Map<BigInteger, byte[]> xset = ((Setup_JDXTHJS) table_array.get(finalJ)).getXset();
                            BigInteger xtag_integer = new BigInteger(xtag);
                            if (xset.containsKey(xtag_integer)) {
                                foundValues.add(xset.get(xtag_integer));
                            }
                            return foundValues;
                        })
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
                ArrayList<byte[]> val_ij = new ArrayList<>(results);
                if (val_ij.isEmpty())
                    break;
                else
                    val_i.add(val_ij);

                if (j == table_num - 1) {
                    for (int m = 0; m < table_num; m++) {
                        ArrayList<String> res_ij = new ArrayList<>();
                        Map<String, Integer> filter_tab = new HashMap<>();
                        ArrayList<String> pos_tab = new ArrayList<>();
                        JDXTHJS_filter_ind(val_i.get(m), Key_List.get(m), filter_tab, pos_tab);
                        for (String s : pos_tab) {
                            if (filter_tab.get(s) < 1)
                                continue;
                            res_ij.add(s);
                        }
                        if (!res_ij.isEmpty())
                            res_i.add(res_ij);
                        else {
                            res_i.clear();
                            break;
                        }
                    }
                }
            }
            if (!res_i.isEmpty())
                res.add(res_i);
        }
        return res;
    }


    public ArrayList<ArrayList<ArrayList<String>>> JDXTHJS_get_res(ArrayList<ArrayList<ArrayList<int[]>>> enc_res, ArrayList<ArrayList<ArrayList<byte[]>>> xtag_array) {
        ArrayList<ArrayList<ArrayList<String>>> res = new ArrayList<>();
        ArrayList<byte[]> K_f_cnt_List = new ArrayList<>();
        ArrayList<byte[]> Key_List = new ArrayList<>();
        int table_num = table_array.size();
        int maxCnt = 0;
        for (int i = 0; i < table_num; i++) {
            maxCnt = Math.max(((Setup_JDXTHJS) table_array.get(i)).getCnt(), maxCnt);
            Key_List.add(Hash.Get_SHA_256((K_r + val_array.get(i) + attr_array.get(i)).getBytes(StandardCharsets.UTF_8)));
        }

        long K_f_tmp = tool.bytesToLong(K_f_bytes);
        for (int i = 0; i < maxCnt; i++) {
            K_f_tmp = Hash.hash64(K_f_tmp, HJS.seed);
            K_f_cnt_List.add(tool.longToBytes(K_f_tmp));
        }

        for (int i = 0; i < xtag_array.size(); i++) {
            ArrayList<ArrayList<String>> res_i = new ArrayList<>();
            ArrayList<ArrayList<byte[]>> val_i = new ArrayList<>();
            for (int j = 0; j < table_num; j++) {
//                ArrayList<byte[]> val_ij = new ArrayList<>();
//                int k_len = xtag_array.get(i).get(j).size();
//                for (int k = 0; k < k_len; k++) {
//                    byte[] xtag = xtag_array.get(i).get(j).get(k);
//                    if (!enc_res.get(i).get(j).isEmpty()) {
//                        int[] ct_indexList = enc_res.get(i).get(j).get(k);
//                        int Cnt = ((Setup_JDXTHJS) table_array.get(j)).getCnt();
//                        for (int m = 0; m < Cnt; m++) {
//                            byte[] tag = tool.Xor(xtag, K_f_cnt_List.get(m));
//                            for (int id : ct_indexList) {
//                                Map<BigInteger, byte[]> ebt_bucket = ((Setup_JDXTHJS) table_array.get(j)).getEBT_array().get(m).get(id);
//                                byte[] val = HJS.HJSTest(tag, ebt_bucket);
//                                if (val != null) {
//                                    // 表示找到val
//                                    val_ij.add(val);
//                                    break;
//                                }
//                            }
//                        }
//                    }
//                    Map<BigInteger, byte[]> xset = ((Setup_JDXTHJS) table_array.get(j)).getXset();
//                    BigInteger xtag_integer = new BigInteger(xtag);
//                    if (xset.containsKey(xtag_integer))
//                        val_ij.add(xset.get(xtag_integer));
//                }

                int k_len = xtag_array.get(i).get(j).size();
                int finalI = i;
                int finalJ = j;
                List<byte[]> results = IntStream.range(0, k_len).parallel()
                        .mapToObj(k -> {
                            byte[] xtag = xtag_array.get(finalI).get(finalJ).get(k);
                            List<byte[]> foundValues = new ArrayList<>();
                            if (!enc_res.get(finalI).get(finalJ).isEmpty()) {
                                int[] ct_indexList = enc_res.get(finalI).get(finalJ).get(k);
                                int Cnt = ((Setup_JDXTHJS) table_array.get(finalJ)).getCnt();
                                IntStream.range(0, Cnt).parallel().forEach(m -> {
                                    byte[] tag = tool.Xor(xtag, K_f_cnt_List.get(m));
                                    for (int id : ct_indexList) {
                                        Map<BigInteger, byte[]> ebt_bucket = ((Setup_JDXTHJS) table_array.get(finalJ))
                                                .getEBT_array().get(m).get(id);
                                        byte[] val = HJS.HJSTest(tag, ebt_bucket);
                                        if (val != null) {
                                            synchronized (foundValues) {
                                                foundValues.add(val);
                                            }
                                            break;
                                        }
                                    }
                                });
                            }
                            Map<BigInteger, byte[]> xset = ((Setup_JDXTHJS) table_array.get(finalJ)).getXset();
                            BigInteger xtag_integer = new BigInteger(xtag);
                            if (xset.containsKey(xtag_integer)) {
                                foundValues.add(xset.get(xtag_integer));
                            }
                            return foundValues;
                        })
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
                ArrayList<byte[]> val_ij = new ArrayList<>(results);
                if (val_ij.isEmpty())
                    break;
                else
                    val_i.add(val_ij);
                if (j == table_num - 1) {
                    for (int m = 0; m < table_num; m++) {
                        TreeSet<String> indSet = new TreeSet<>();
                        for (byte[] val : val_i.get(m)) {
                            byte[] record_byte = tool.Xor(Key_List.get(m), val);
                            String record = new String(record_byte);
                            String[] ind_op = record.split("\\|");
                            indSet.add(ind_op[0]);
                        }
                        ArrayList<String> res_ij = new ArrayList<>(indSet);
                        res_i.add(res_ij);
                    }
                }
            }
            if (!res_i.isEmpty())
                res.add(res_i);
        }
        return res;
    }


    public String getInd() {
        return ind;
    }

    public String getW() {
        return w;
    }

    public String getOp() {
        return op;
    }

    public ArrayList<Jointuple> getJoinArray() {
        return joinArray;
    }

    public ArrayList<byte[]> getStokenList() {
        return stokenList;
    }

    public GGMXTokenList getGgmxTokenList() {
        return ggmxTokenList;
    }

    public int getPrior_pos() {
        return prior_pos;
    }
}
