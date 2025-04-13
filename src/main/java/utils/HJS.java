package utils;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class HJS {
    public static int k = 2;
    public static long seed = 4522196413070268060L;
    private static int bits = 20000;

//    public static String genHexString(int len, ThreadLocalRandom random) {
//        StringBuilder sb = new StringBuilder(len);
//        for (int i = 0; i < len; i++) {
//            int randomInt = random.nextInt(16);
//            sb.append(Integer.toHexString(randomInt));
//        }
//        String str = sb.toString();
//        return str;
//    }

    public static ArrayList<Map<BigInteger, byte[]>> HJSEncrypt(byte[] K_f_bytes, Map<BigInteger, byte[]> map) {
        ArrayList<Map<BigInteger, byte[]>> EBT = new ArrayList<>();
        for (int i = 0; i < bits; i++) {
           Map<BigInteger, byte[]> EBT_bucket = new HashMap<>();
            EBT.add(EBT_bucket);
        }
        ThreadLocalRandom random = ThreadLocalRandom.current();
        ArrayList<Long> mod = new ArrayList<>();
        for (int i = 0; i <= k; i++)
            mod.add((long) (bits / k) * i);
        for (Map.Entry<BigInteger, byte[]> entry : map.entrySet()) {
            byte[] xtag = entry.getKey().toByteArray();// get xtag
            byte[] val = entry.getValue();
            int randk = random.nextInt(1, k + 1);
            int pos = 0;
            long hash = Hash.hash64(tool.bytesToLong(xtag), seed);
            long a = (hash >>> 32) | (hash << 32);
            pos = (int) (Math.abs(a) % (mod.get(randk)));
            byte[] tag = tool.Xor(xtag, K_f_bytes);
            EBT.get(pos).put(new BigInteger(tag), val);
        }

//        TreeSet<Integer> treeSet = new TreeSet<>();
//        for (ArrayList<HJStuple> ebt : EBT)
//            treeSet.add(ebt.size());
//        int pi = treeSet.last();
        // padding operation
//        for (int i = 0; i < bits; i++) {
//            ArrayList<HJStuple> ebt = EBT.get(i);
//            int ebtsize = ebt.size();
//            if (ebtsize == 0) {
//                while (ebtsize < 10) {
//                    String tag_str = genHexString(32, random);
//                    String val_str = genHexString(32, random);
//                    byte[] tag = tag_str.getBytes(StandardCharsets.UTF_8);
//                    byte[] val = val_str.getBytes(StandardCharsets.UTF_8);
//                    ebt.add(new HJStuple(tag, val));
//                    tag_str = null;
//                    val_str = null;
//                    ebtsize++;
//                }
//            }
//        }
//        treeSet = null;
        return EBT;
    }

    public static int[] HJSRespond(byte[] xtag) {
        int[] res = new int[k];
        ArrayList<Long> mod = new ArrayList<>();
        for (int i = 0; i <= k; i++)
            mod.add((long) (bits / k) * i);
        int pos = 0;
        long hash = Hash.hash64(tool.bytesToLong(xtag), seed);
        long a = (hash >>> 32) | (hash << 32);
        for (int i = 1; i <= k; i++) {
            pos = (int) (Math.abs(a) % (mod.get(i)));
            res[i - 1] = pos;
        }
        return res;
    }

    public static ArrayList<Map<BigInteger, byte[]>> HJSRespond_bucket(byte[] xtag, ArrayList<Map<BigInteger, byte[]>> ebt) {
        ArrayList<Map<BigInteger, byte[]>> res = new ArrayList<>();
        ArrayList<Long> mod = new ArrayList<>();
        for (int i = 0; i <= k; i++)
            mod.add((long) (bits / k) * i);
        int pos;
        long hash = Hash.hash64(tool.bytesToLong(xtag), seed);
        long a = (hash >>> 32) | (hash << 32);
        for (int i = 1; i <= k; i++) {
            pos = (int) (Math.abs(a) % (mod.get(i)));
            res.add(ebt.get(pos));
        }
        return res;
    }

    public static byte[] HJSTest(byte[] tag, Map<BigInteger, byte[]> res) {
        BigInteger tag_int = new BigInteger(tag);
        if (res.containsKey(tag_int))
            return res.get(tag_int);
        return null;
    }
}
