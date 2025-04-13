import client.Setup_JDXT;
import client.Setup_JDXTHJS;
import server.Server_JDXT;
import server.Server_JDXTHJS;
import utils.Bloom;
import utils.Restuple;
import utils.query;

import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Map;

public class test_del {//deletion 20% 40% 60% 80%
    public static void main(String[] args) throws NoSuchAlgorithmException, InvalidKeyException {
        int key_column = 9;
        int join_column = 1;
        int record_num = (int) Math.pow(2, 16);
        int table_num = 2;//the number of the queried tables
        String condition = "_del";
        // del 20% 40% 60% 80%
        for (int i = 20; i <= 80; i += 20) {
            System.out.println("------------- del = " + i + "%-------------");

            ArrayList<String> val_array = new ArrayList<>();
            ArrayList<String> attr_array = new ArrayList<>();
            for (int j = 0; j < table_num; j++) {
                val_array.add("table" + (j + 1) + "_keyword_0_0");
                attr_array.add("join-attr0");
            }

            {
                ArrayList<Object> tablearray = new ArrayList<>();
                for (int j = 1; j <= table_num; j++) {
                    Setup_JDXT tab = new Setup_JDXT(join_column);
                    tab.construct(j, key_column, record_num, condition + i);
                    tablearray.add(tab);
                }
                long search_all = 0;
                long decrypt_all = 0;
                long server_all = 0;
                long tokenprior_all = 0;
                for (int x = 0; x < 100; x++) {
                    long search_start = System.nanoTime();

                    query joinquery = new query(tablearray, val_array, attr_array);
                    joinquery.JDXT_getJoinToken();
                    long tokenprior_end = System.nanoTime();
                    int prior_pos = joinquery.getPrior_pos();

                    Map<BigInteger, byte[]> tset = ((Setup_JDXT) tablearray.get(prior_pos)).getTset();
                    ArrayList<Bloom> f_array = new ArrayList<>();
                    ArrayList<Map<BigInteger, byte[]>> cset_array = new ArrayList<>();
                    for (int j = 0; j < table_num; j++) {
                        f_array.add(((Setup_JDXT) tablearray.get(j)).getF());
                        cset_array.add(((Setup_JDXT) tablearray.get(j)).getCset());
                    }

                    Server_JDXT server_jdxt = new Server_JDXT(tset, f_array, cset_array);

                    long server_start = System.nanoTime();
                    ArrayList<ArrayList<ArrayList<byte[]>>> encRes = server_jdxt.search(joinquery.getStokenList(), joinquery.getGgmxTokenList());
                    long server_end = System.nanoTime();

                    long decrypt_start = System.nanoTime();
                    ArrayList<ArrayList<ArrayList<String>>> res = joinquery.JDXT_filter_res(encRes);
                    long decrypt_end = System.nanoTime();
                    long search_end = System.nanoTime();
                    search_all += search_end - search_start;
                    decrypt_all += decrypt_end - decrypt_start;
                    server_all += server_end - server_start;
                    tokenprior_all += tokenprior_end - search_start;
                }
                System.out.println("-------------JDXT-------------");
                System.out.println("JDXT average search time : " + search_all / Math.pow(10, 6 + 2) + " ms");
                System.out.println("JDXT average decrypt time : " + decrypt_all / Math.pow(10, 6 + 2) + " ms");
                System.out.println("JDXT average server time : " + server_all / Math.pow(10, 6 + 2) + " ms");
                System.out.println("JDXT average token and prior time : " + tokenprior_all / Math.pow(10, 6 + 2) + " ms");
            }

            {
                ArrayList<Object> tablearray = new ArrayList<>();
                for (int j = 1; j <= table_num; j++) {
                    Setup_JDXTHJS tab = new Setup_JDXTHJS(join_column, j);
                    tab.construct(key_column, record_num, condition + i);
                    tablearray.add(tab);
                }
                long search_all = 0;
                long decrypt_all = 0;
                long server_all = 0;
                long tokenprior_all = 0;
                for (int x = 0; x < 100; x++) {
                    long search_start = System.nanoTime();
                    query joinquery = new query(tablearray, val_array, attr_array);
                    joinquery.JDXTHJS_getJoinToken();
                    long tokenprior_end = System.nanoTime();

                    int prior_pos = joinquery.getPrior_pos();
                    Map<BigInteger, byte[]> tset = ((Setup_JDXTHJS) tablearray.get(prior_pos)).getTset();
                    ArrayList<Integer> table_EBTList = new ArrayList<>();//存储每个表有几个ebt
                    for (int j = 0; j < table_num; j++) {
                        table_EBTList.add(((Setup_JDXTHJS) tablearray.get(j)).getCnt());
                    }

                    Server_JDXTHJS server_jdxthjs = new Server_JDXTHJS(tset, table_EBTList);

                    long server_start = System.nanoTime();
                    Restuple encRes = server_jdxthjs.search(joinquery.getStokenList(), joinquery.getGgmxTokenList());
                    long server_end = System.nanoTime();

                    ArrayList<ArrayList<ArrayList<String>>> res = joinquery.JDXTHJS_filter_res_parallel(encRes.encRes, encRes.xtag_array);

                    long search_end = System.nanoTime();

                    search_all += search_end - search_start;
                    decrypt_all += search_end - server_end;
                    server_all += server_end - server_start;
                    tokenprior_all += tokenprior_end - search_start;
                }
                System.out.println("-------------JDXTHJS-------------");
                System.out.println("JDXTHJS average search time : " + search_all / Math.pow(10, 6 + 2) + " ms");
                System.out.println("JDXTHJS average decrypt time : " + decrypt_all / Math.pow(10, 6 + 2) + " ms");
                System.out.println("JDXTHJS average server time : " + server_all / Math.pow(10, 6 + 2) + " ms");
                System.out.println("JDXTHJS average token and prior time : " + tokenprior_all / Math.pow(10, 6 + 2) + " ms");
            }
        }
    }
}
