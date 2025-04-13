import client.*;
import server.*;
import utils.*;

import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Map;

public class test_join {
    public static void main(String[] args) throws NoSuchAlgorithmException, InvalidKeyException {
        int record_num = (int) Math.pow(2, 16);
        int key_column = 9;
        int join_column = 1;
        String condition = "_Lmax1000";

        for (int table_num = 2; table_num <= 6; table_num++) {
            ArrayList<String> val_array = new ArrayList<>();
            ArrayList<String> attr_array = new ArrayList<>();
            for (int j = 0; j < table_num; j++) {
                val_array.add("table1_keyword_9_0");
                attr_array.add("join-attr0");
            }
            System.out.println("table_num = " + table_num + ":");


            {
                // JDXT
                ArrayList<Object> tablearray = new ArrayList<>();
                for (int j = 1; j <= table_num; j++) {
                    Setup_JDXT tab = new Setup_JDXT(join_column);
                    tab.construct(j, key_column, record_num, condition);
                    tablearray.add(tab);
                }

                long search_all = 0;
                for (int x = 0; x < 100; x++) {
                    long search_start = System.nanoTime();

                    query joinquery = new query(tablearray, val_array, attr_array);
                    joinquery.JDXT_getJoinToken();
                    int prior_pos = joinquery.getPrior_pos();

                    Map<BigInteger, byte[]> tset = ((Setup_JDXT) tablearray.get(prior_pos)).getTset();
                    ArrayList<Bloom> f_array = new ArrayList<>();
                    ArrayList<Map<BigInteger, byte[]>> cset_array = new ArrayList<>();
                    for (int j = 0; j < table_num; j++) {
                        f_array.add(((Setup_JDXT) tablearray.get(j)).getF());
                        cset_array.add(((Setup_JDXT) tablearray.get(j)).getCset());
                    }

                    Server_JDXT server_jdxt = new Server_JDXT(tset, f_array, cset_array);

                    ArrayList<ArrayList<ArrayList<byte[]>>> encRes = server_jdxt.search(joinquery.getStokenList(), joinquery.getGgmxTokenList());

                    ArrayList<ArrayList<ArrayList<String>>> res = joinquery.JDXT_get_res(encRes);

                    long search_end = System.nanoTime();
//                    if (x == 0) {
//                        for (int i = 0; i < res.size(); i++) {
//                            System.out.println("----------------" + i + "----------------");
//                            for (int j = 0; j < res.get(i).size(); j++) {
//                                System.out.println(i + ": " + j + "table result:");
//                                for (int k = 0; k < res.get(i).get(j).size(); k++)
//                                    System.out.print(res.get(i).get(j).get(k) + ", ");
//                                System.out.println();
//                            }
//                        }
//                    }
                    search_all += search_end - search_start;
                }
                System.out.println("-------------JDXT-------------");
                System.out.println("JDXT average search time : " + search_all / Math.pow(10, 6 + 2) + " ms");
            }

            {
                //JDXTEMM
                ArrayList<Object> tablearray = new ArrayList<>();
                for (int j = 1; j <= table_num; j++) {
                    Setup_JDXTEMM tab = new Setup_JDXTEMM(join_column);
                    tab.construct(j, key_column, record_num, condition);
                    tablearray.add(tab);
                }
                long search_all = 0;
                for (int x = 0; x < 100; x++) {
                    long search_start = System.nanoTime();

                    query joinquery = new query(tablearray, val_array, attr_array);
                    joinquery.JDXTEMM_getJoinToken();
                    int prior_pos = joinquery.getPrior_pos();

                    Map<BigInteger, byte[]> tset = ((Setup_JDXTEMM) tablearray.get(prior_pos)).getTset();
                    ArrayList<Bloom> f_array = new ArrayList<>();
                    ArrayList<Map<BigInteger, ArrayList<byte[]>>> cset_array = new ArrayList<>();
                    for (int j = 0; j < table_num; j++) {
                        f_array.add(((Setup_JDXTEMM) tablearray.get(j)).getF());
                        cset_array.add(((Setup_JDXTEMM) tablearray.get(j)).getCset());
                    }

                    Server_JDXTEMM server_jdxt = new Server_JDXTEMM(tset, f_array, cset_array);

                    ArrayList<ArrayList<ArrayList<byte[]>>> encRes = server_jdxt.search(joinquery.getStokenList(), joinquery.getGgmxTokenList());

                    ArrayList<ArrayList<ArrayList<String>>> res = joinquery.JDXT_get_res(encRes);
                    long search_end = System.nanoTime();
                    search_all += search_end - search_start;
                }
                System.out.println("-------------JDXTEMM-------------");
                System.out.println("JDXTEMM average search time : " + search_all / Math.pow(10, 6 + 2) + " ms");
            }


        }
    }
}
