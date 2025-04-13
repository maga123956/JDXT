package client;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import utils.*;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.math.BigInteger;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class Setup_JDXTEMM {
    private static String K_r = "8975924566f6e252";
    private static String K_x = "89b7a92966f6eb32";
    private static String K_y = "7975922666f6eb02";
    private static String K_z = "9862192ad6f6ef65";
    private static String K_z1 = "9874a22554e7db85";
    private static String K_c = "787599ac86f2e82";
    private int join_column;
    private Map<BigInteger, byte[]> tset = new LinkedHashMap<>();//tset
    private Map<BigInteger, ArrayList<byte[]>> cset = new LinkedHashMap<>();//cset
    private Bloom f;
    private Map<String, Integer> C = new HashMap<>();
    private ArrayList<Long> xy_array;

    private GGMTree ggmTree;
    private int delta = 11000;

    public Setup_JDXTEMM(int join_column_num) {
        join_column = join_column_num;
        xy_array = new ArrayList<>();
        ggmTree = new GGMTree(delta);
    }

    public void construct(int table_id, int key_column, int record_num, String condition) {
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

                        byte[] w_cnt_rootKey = Hash.Get_SHA_256((K_z + w).getBytes(StandardCharsets.UTF_8));
                        byte[] w_cnt = new byte[Cryptoutils.AES_BLOCK_SIZE];
                        System.arraycopy(w_cnt_rootKey, 0, w_cnt, 0, Cryptoutils.AES_BLOCK_SIZE);
                        ggmTree.derive_key_from_tree(w_cnt, w_c, 0, ggmTree.getLevel());

                        byte[] w1 = Hash.Get_SHA_256((K_z1 + w).getBytes(StandardCharsets.UTF_8));
                        byte[] msg = (ind + "|" + op).getBytes(StandardCharsets.UTF_8);//ind||op
                        for (Jointuple jointuple : joinArray) {
                            String attr_ = jointuple.attr_;
                            String w_ = jointuple.w_;
                            byte[] addr = Hash.Get_SHA_256((K_r + w + w_c + attr_).getBytes(StandardCharsets.UTF_8));//addr
                            byte[] val = tool.Xor(msg, Objects.requireNonNull(Hash.Get_SHA_256((K_r + w + attr_).getBytes(StandardCharsets.UTF_8))));
                            byte[] w_tmp = Hash.Get_SHA_256((K_y + w_).getBytes(StandardCharsets.UTF_8));
                            byte[] alpha = tool.Xor(Objects.requireNonNull(w_tmp), w_cnt);
                            byte[] xtag = tool.Xor(tool.Xor(w_tmp, Objects.requireNonNull(Hash.Get_SHA_256((K_c + 1).getBytes(StandardCharsets.UTF_8)))), tool.Xor(w1, Objects.requireNonNull(Hash.Get_SHA_256((K_x + attr_).getBytes(StandardCharsets.UTF_8)))));
                            tset.put(new BigInteger(addr), alpha);
                            ArrayList<byte[]> valList;
                            BigInteger xtag_integer = new BigInteger(xtag);
                            if (!cset.containsKey(xtag_integer)) {
                                valList = new ArrayList<>();
                                xy_array.add(tool.bytesToLong(xtag));
                            } else
                                valList = cset.get(xtag_integer);
                            valList.add(val);
                            cset.put(xtag_integer, valList);
                        }
                    }
                }
                counter++;
            }
            long[] xy = new long[xy_array.size()];
            for (int i = 0; i < xy_array.size(); i++)
                xy[i] = xy_array.get(i);
            f = Bloom.construct(xy, 64);
//            xy_array.clear();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InvalidKeyException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }


    public void JDXTEMM_update(query query_) throws NoSuchAlgorithmException, InvalidKeyException {
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
        byte[] w_cnt = new byte[Cryptoutils.AES_BLOCK_SIZE];
        System.arraycopy(w_cnt_rootKey, 0, w_cnt, 0, Cryptoutils.AES_BLOCK_SIZE);
        ggmTree.derive_key_from_tree(w_cnt, w_c, 0, ggmTree.getLevel());

        byte[] w1 = Hash.Get_SHA_256((K_z1 + w).getBytes(StandardCharsets.UTF_8));
        for (Jointuple jointuple : joinArray) {
            String attr_ = jointuple.attr_;
            String w_ = jointuple.w_;
            byte[] addr = Hash.Get_SHA_256((K_r + w + w_c + attr_).getBytes(StandardCharsets.UTF_8));//addr
            byte[] val = tool.Xor(msg, Objects.requireNonNull(Hash.Get_SHA_256((K_r + w + attr_).getBytes(StandardCharsets.UTF_8))));
            byte[] w_tmp = Hash.Get_SHA_256((K_y + w_).getBytes(StandardCharsets.UTF_8));
            byte[] alpha = tool.Xor(Objects.requireNonNull(w_tmp), w_cnt);
            byte[] xtag = tool.Xor(tool.Xor(w_tmp, Objects.requireNonNull(Hash.Get_SHA_256((K_c + 1).getBytes(StandardCharsets.UTF_8)))), tool.Xor(w1, Objects.requireNonNull(Hash.Get_SHA_256((K_x + attr_).getBytes(StandardCharsets.UTF_8)))));
            tset.put(new BigInteger(addr), alpha);
            ArrayList<byte[]> valList;
            BigInteger xtag_integer = new BigInteger(xtag);
            if (!cset.containsKey(xtag_integer)) {
                valList = new ArrayList<>();
                xy_array.add(tool.bytesToLong(xtag));
            } else
                valList = cset.get(xtag_integer);
            valList.add(val);
            cset.put(xtag_integer, valList);
        }
        long[] xy = new long[xy_array.size()];
        for (int i = 0; i < xy_array.size(); i++)
            xy[i] = xy_array.get(i);
        f = Bloom.construct(xy, 64);
    }

    public void JDXTEMM_update_batch(ArrayList<query> queryList) throws NoSuchAlgorithmException, InvalidKeyException {
        for(query query_: queryList){
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
            byte[] w_cnt = new byte[Cryptoutils.AES_BLOCK_SIZE];
            System.arraycopy(w_cnt_rootKey, 0, w_cnt, 0, Cryptoutils.AES_BLOCK_SIZE);
            ggmTree.derive_key_from_tree(w_cnt, w_c, 0, ggmTree.getLevel());

            byte[] w1 = Hash.Get_SHA_256((K_z1 + w).getBytes(StandardCharsets.UTF_8));
            for (Jointuple jointuple : joinArray) {
                String attr_ = jointuple.attr_;
                String w_ = jointuple.w_;
                byte[] addr = Hash.Get_SHA_256((K_r + w + w_c + attr_).getBytes(StandardCharsets.UTF_8));//addr
                byte[] val = tool.Xor(msg, Objects.requireNonNull(Hash.Get_SHA_256((K_r + w + attr_).getBytes(StandardCharsets.UTF_8))));
                byte[] w_tmp = Hash.Get_SHA_256((K_y + w_).getBytes(StandardCharsets.UTF_8));
                byte[] alpha = tool.Xor(Objects.requireNonNull(w_tmp), w_cnt);
                byte[] xtag = tool.Xor(tool.Xor(w_tmp, Objects.requireNonNull(Hash.Get_SHA_256((K_c + 1).getBytes(StandardCharsets.UTF_8)))), tool.Xor(w1, Objects.requireNonNull(Hash.Get_SHA_256((K_x + attr_).getBytes(StandardCharsets.UTF_8)))));
                tset.put(new BigInteger(addr), alpha);
                ArrayList<byte[]> valList;
                BigInteger xtag_integer = new BigInteger(xtag);
                if (!cset.containsKey(xtag_integer)) {
                    valList = new ArrayList<>();
                    xy_array.add(tool.bytesToLong(xtag));
                } else
                    valList = cset.get(xtag_integer);
                valList.add(val);
                cset.put(xtag_integer, valList);
            }
        }
        long[] xy = new long[xy_array.size()];
        for (int i = 0; i < xy_array.size(); i++)
            xy[i] = xy_array.get(i);
        f = Bloom.construct(xy, 64);
    }

    public void Store(String text) {
        try {
            FileOutputStream file = new FileOutputStream("data/EDB/JDXTEMM_" + text + ".dat");
            // tset store
            for (BigInteger stag : tset.keySet()) {
                file.write(stag.toByteArray());
                file.write(tset.get(stag));
            }

            // xset store
            byte[][] xset_bytes = f.getData();
            for (byte[] xtag : xset_bytes)
                file.write(xtag);

            // cset store
            for (BigInteger xtag : cset.keySet()) {
                file.write(xtag.toByteArray());
                ArrayList<byte[]> val_array = cset.get(xtag);
                for (byte[] val : val_array)
                    file.write(val);
            }

            FileChannel channel = file.getChannel();
            System.out.println("JDXTEMM file size is " + (double)channel.size() / 1024.0 / 1024.0 + "MB");
            channel.close();
            file.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, Integer> getC() {
        return C;
    }

    public Map<BigInteger, ArrayList<byte[]>> getCset() {
        return cset;
    }

    public Bloom getF() {
        return f;
    }

    public Map<BigInteger, byte[]> getTset() {
        return tset;
    }
}
