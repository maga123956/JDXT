package client;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import utils.*;

import java.io.*;
import java.math.BigInteger;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class Setup_JDXTHJS {
    private static String K_r = "8975924566f6e252";
    private static String K_x = "89b7a92966f6eb32";
    private static String K_y = "7975922666f6eb02";
    private static String K_z = "9862192ad6f6ef65";
    private static String K_z1 = "9874a22554e7db85";
    private static String K_f = "6574b33984e7fb55";
    private static byte[] K_f_bytes;
    private int join_column;
    private Map<BigInteger, byte[]> tset = new LinkedHashMap<>();//tset
    private Map<BigInteger, byte[]> xset = new LinkedHashMap<>();//xset, 客户端缓存
    private ArrayList<ArrayList<Map<BigInteger, byte[]>>> EBT_array;//EBT_array
    private Map<String, Integer> C = new HashMap<>();
    private int p = 100000;//the maximum capacity of XSet
    private int cnt = 0;
    private int table_id;

    private GGMTree ggmTree;
    private int delta = 11000;

    public Setup_JDXTHJS(int join_column_num, int table_id) {
        join_column = join_column_num;
        EBT_array = new ArrayList<>();
        this.table_id = table_id;
        ggmTree = new GGMTree(delta);
        K_f_bytes = K_f.getBytes(StandardCharsets.UTF_8);
    }

    public void construct(int key_column, int record_num, String condition) {
        // 构造path
        String path = "data/table" + table_id + "/table" + table_id + "_k" + key_column
                + "_j" + join_column + "_" + record_num + condition + ".csv";
        ArrayList<String> joinColumn_List = new ArrayList<>();
        try (Reader reader = Files.newBufferedReader(Paths.get(path))) {
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(reader);
            int counter = 0;
            for (CSVRecord record : records) {
                if (counter == 0) {
                    for (int i = 0; i < join_column; i++)
                        joinColumn_List.add(record.get(key_column + i + 1));
                } else {
                    ArrayList<Jointuple> joinArray = new ArrayList<>();
                    for (int i = 0; i < join_column; i++) {
                        Jointuple jointuple = new Jointuple(joinColumn_List.get(i), record.get(key_column + i + 1));
                        joinArray.add(jointuple);
                    }
                    for (int j = 0; j < key_column; j++) {
                        String ind, op;
                        if (record.get(0).contains("||")) {
                            String[] ind_op = record.get(0).split("\\|\\|");
                            ind = ind_op[0];
                            op = ind_op[1];
                        } else {
                            ind = record.get(0);
                            op = "add";
                        }
                        String w = record.get(j + 1);
                        if (!C.containsKey(w))
                            C.put(w, 0);
                        int w_c = C.get(w) + 1;
                        C.put(w, w_c);
                        byte[] msg = (ind + "|" + op).getBytes(StandardCharsets.UTF_8);//ind||op
                        byte[] w_cnt_rootKey = Hash.Get_SHA_256((K_z + w).getBytes(StandardCharsets.UTF_8));
                        byte[] w1_cnt_rootKey = Hash.Get_SHA_256((K_z1 + w).getBytes(StandardCharsets.UTF_8));
                        byte[] w_cnt = new byte[Cryptoutils.AES_BLOCK_SIZE];
                        byte[] w1_cnt = new byte[Cryptoutils.AES_BLOCK_SIZE];
                        System.arraycopy(w_cnt_rootKey, 0, w_cnt, 0, Cryptoutils.AES_BLOCK_SIZE);
                        System.arraycopy(w1_cnt_rootKey, 0, w1_cnt, 0, Cryptoutils.AES_BLOCK_SIZE);
                        ggmTree.derive_key_from_tree(w_cnt, w_c, 0, ggmTree.getLevel());
                        ggmTree.derive_key_from_tree(w1_cnt, w_c, 0, ggmTree.getLevel());

                        for (Jointuple jointuple : joinArray) {
                            String attr_ = jointuple.attr_;
                            String w_ = jointuple.w_;
                            byte[] addr = Hash.Get_SHA_256((K_r + w + w_c + attr_).getBytes(StandardCharsets.UTF_8));//addr
                            byte[] val = tool.Xor(msg, Objects.requireNonNull(Hash.Get_SHA_256((K_r + w + attr_).getBytes(StandardCharsets.UTF_8))));
                            byte[] w_tmp = Hash.Get_SHA_256((K_y + w_).getBytes(StandardCharsets.UTF_8));
                            byte[] alpha = tool.Xor(Objects.requireNonNull(w_tmp), w_cnt);
                            byte[] xtag = tool.Xor(w_tmp, tool.Xor(w1_cnt, Objects.requireNonNull(Hash.Get_SHA_256((K_x + attr_).getBytes(StandardCharsets.UTF_8)))));
                            tset.put(new BigInteger(addr), alpha);
                            xset.put(new BigInteger(xtag), val);
                        }
                    }
                }
                counter++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InvalidKeyException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        if (xset.size() >= p) {
            cnt++;
            long tmp = tool.bytesToLong(K_f_bytes);
            for (int i = 1; i <= cnt; i++)
                tmp = Hash.hash64(tmp, HJS.seed);
            ArrayList<Map<BigInteger, byte[]>> EBT = HJS.HJSEncrypt(tool.longToBytes(tmp), xset);
            EBT_array.add(EBT);
            xset.clear();
        }
    }

    public void JDXTHJS_update(query query_) throws NoSuchAlgorithmException, InvalidKeyException {
        String ind = query_.getInd();
        String w = query_.getW();
        String op = query_.getOp();
        ArrayList<Jointuple> joinArray = query_.getJoinArray();
        if (join_column != joinArray.size()) {
            System.out.println("Warning: join_column does not match joinArray size.");
            return;
        }
        if (!C.containsKey(w))
            C.put(w, 0);
        int w_c = C.get(w) + 1;
        C.put(w, w_c);

        byte[] msg = (ind + "|" + op).getBytes(StandardCharsets.UTF_8);//ind||op
        byte[] w_cnt_rootKey = Hash.Get_SHA_256((K_z + w).getBytes(StandardCharsets.UTF_8));
        byte[] w1_cnt_rootKey = Hash.Get_SHA_256((K_z1 + w).getBytes(StandardCharsets.UTF_8));
        byte[] w_cnt = new byte[Cryptoutils.AES_BLOCK_SIZE];
        byte[] w1_cnt = new byte[Cryptoutils.AES_BLOCK_SIZE];
        System.arraycopy(w_cnt_rootKey, 0, w_cnt, 0, Cryptoutils.AES_BLOCK_SIZE);
        System.arraycopy(w1_cnt_rootKey, 0, w1_cnt, 0, Cryptoutils.AES_BLOCK_SIZE);
        ggmTree.derive_key_from_tree(w_cnt, w_c, 0, ggmTree.getLevel());
        ggmTree.derive_key_from_tree(w1_cnt, w_c, 0, ggmTree.getLevel());
        for (Jointuple jointuple : joinArray) {
            String attr_ = jointuple.attr_;
            String w_ = jointuple.w_;
            byte[] addr = Hash.Get_SHA_256((K_r + w + w_c + attr_).getBytes(StandardCharsets.UTF_8));//addr
            byte[] val = tool.Xor(msg, Objects.requireNonNull(Hash.Get_SHA_256((K_r + w + attr_).getBytes(StandardCharsets.UTF_8))));
            byte[] w_tmp = Hash.Get_SHA_256((K_y + w_).getBytes(StandardCharsets.UTF_8));
            byte[] alpha = tool.Xor(Objects.requireNonNull(w_tmp), w_cnt);
            byte[] xtag = tool.Xor(w_tmp, tool.Xor(w1_cnt, Objects.requireNonNull(Hash.Get_SHA_256((K_x + attr_).getBytes(StandardCharsets.UTF_8)))));
            tset.put(new BigInteger(addr), alpha);
            xset.put(new BigInteger(xtag), val);
        }

        if (xset.size() >= p) {
            cnt++;
            long tmp = tool.bytesToLong(K_f_bytes);
            for (int i = 1; i <= cnt; i++)
                tmp = Hash.hash64(tmp, HJS.seed);
            ArrayList<Map<BigInteger, byte[]>> EBT = HJS.HJSEncrypt(tool.longToBytes(tmp), xset);
            EBT_array.add(EBT);
            xset.clear();
        }
    }

    public void JDXTHJS_update_batch(ArrayList<query> queryList) throws NoSuchAlgorithmException, InvalidKeyException {
        for (query query_ : queryList) {
            String ind = query_.getInd();
            String w = query_.getW();
            String op = query_.getOp();
            ArrayList<Jointuple> joinArray = query_.getJoinArray();
            if (join_column != joinArray.size()) {
                System.out.println("Warning: join_column does not match joinArray size.");
                return;
            }
            if (!C.containsKey(w))
                C.put(w, 0);
            int w_c = C.get(w) + 1;
            C.put(w, w_c);

            byte[] msg = (ind + "|" + op).getBytes(StandardCharsets.UTF_8);//ind||op
            byte[] w_cnt_rootKey = Hash.Get_SHA_256((K_z + w).getBytes(StandardCharsets.UTF_8));
            byte[] w1_cnt_rootKey = Hash.Get_SHA_256((K_z1 + w).getBytes(StandardCharsets.UTF_8));
            byte[] w_cnt = new byte[Cryptoutils.AES_BLOCK_SIZE];
            byte[] w1_cnt = new byte[Cryptoutils.AES_BLOCK_SIZE];
            System.arraycopy(w_cnt_rootKey, 0, w_cnt, 0, Cryptoutils.AES_BLOCK_SIZE);
            System.arraycopy(w1_cnt_rootKey, 0, w1_cnt, 0, Cryptoutils.AES_BLOCK_SIZE);
            ggmTree.derive_key_from_tree(w_cnt, w_c, 0, ggmTree.getLevel());
            ggmTree.derive_key_from_tree(w1_cnt, w_c, 0, ggmTree.getLevel());
            for (Jointuple jointuple : joinArray) {
                String attr_ = jointuple.attr_;
                String w_ = jointuple.w_;
                byte[] addr = Hash.Get_SHA_256((K_r + w + w_c + attr_).getBytes(StandardCharsets.UTF_8));//addr
                byte[] val = tool.Xor(msg, Objects.requireNonNull(Hash.Get_SHA_256((K_r + w + attr_).getBytes(StandardCharsets.UTF_8))));
                byte[] w_tmp = Hash.Get_SHA_256((K_y + w_).getBytes(StandardCharsets.UTF_8));
                byte[] alpha = tool.Xor(Objects.requireNonNull(w_tmp), w_cnt);
                byte[] xtag = tool.Xor(w_tmp, tool.Xor(w1_cnt, Objects.requireNonNull(Hash.Get_SHA_256((K_x + attr_).getBytes(StandardCharsets.UTF_8)))));
                tset.put(new BigInteger(addr), alpha);
                xset.put(new BigInteger(xtag), val);
            }
        }
        if (xset.size() >= p) {
            cnt++;
            long tmp = tool.bytesToLong(K_f_bytes);
            for (int i = 1; i <= cnt; i++)
                tmp = Hash.hash64(tmp, HJS.seed);
            ArrayList<Map<BigInteger, byte[]>> EBT = HJS.HJSEncrypt(tool.longToBytes(tmp), xset);
            EBT_array.add(EBT);
            xset.clear();
        }
    }

    public void Store(String text) {
        try {
            FileOutputStream file = new FileOutputStream("data/EDB/JDXTHJS_" + text + ".dat");
            // tset store
            for (BigInteger stag : tset.keySet()) {
                file.write(stag.toByteArray());
                file.write(tset.get(stag));
            }

            // xset store
            for (BigInteger xtag : xset.keySet()) {
                file.write(xtag.toByteArray());
                file.write(xset.get(xtag));
            }

            // EBT_array store
            for (int i = 0; i < EBT_array.size(); i++) {
                for (int j = 0; j < EBT_array.get(i).size(); j++) {
                    Map<BigInteger, byte[]> bucket = EBT_array.get(i).get(j);
                    for (BigInteger tag : bucket.keySet()) {
                        file.write(tag.toByteArray());
                        file.write(bucket.get(tag));
                    }
                }
            }
            FileChannel channel = file.getChannel();
            System.out.println("JDXTHJS file size is " + (double) channel.size() / 1024.0 / 1024.0 + "MB");
            channel.close();
            file.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<BigInteger, byte[]> getTset() {
        return tset;
    }

    public Map<String, Integer> getC() {
        return C;
    }

    public int getCnt() {
        return cnt;
    }

    public Map<BigInteger, byte[]> getXset() {
        return xset;
    }

    public ArrayList<ArrayList<Map<BigInteger, byte[]>>> getEBT_array() {
        return EBT_array;
    }
}
