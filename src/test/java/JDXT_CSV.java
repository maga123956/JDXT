import client.Setup_JDXT;
import server.Server_JDXT;
import utils.Bloom;
import utils.query;

import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Map;

public class JDXT_CSV {
    public static void main(String[] args) throws NoSuchAlgorithmException, InvalidKeyException {
        // Step 1 prepare the parameters for table
        int key_column = 9;
        int join_column = 1;
        int record_num = (int) Math.pow(2, 16);
        int table_num = 2;//the number of the queried tables
        String condition = "";

        // Step 2 begin to set up
        System.out.println("------------- JDXT ---------------");
        System.out.println("---------- JDXT setup ------------");
        long setup_start = System.nanoTime();
        ArrayList<Object> tablearray = new ArrayList<>();
        for (int i = 1; i <= table_num; i++) {
            Setup_JDXT tab = new Setup_JDXT(join_column);
            tab.construct(i, key_column, record_num, condition);
            tablearray.add(tab);
        }
        long setup_end = System.nanoTime();
        System.out.println("JDXT setup time : " + (setup_end - setup_start) / Math.pow(10, 6) + " ms");

        // Step 3 join operation
        ArrayList<String> val_array = new ArrayList<>();
        ArrayList<String> attr_array = new ArrayList<>();
        for (int i = 0; i < table_num; i++) {
            val_array.add("table" + (i + 1) + "_keyword_0_0");
            attr_array.add("join-attr0");
        }


        long search_all = 0;
        long decrypt_all = 0;
        long server_all = 0;
        long searchprepare_all = 0;
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
            for (int i = 0; i < table_num; i++) {
                f_array.add(((Setup_JDXT) tablearray.get(i)).getF());
                cset_array.add(((Setup_JDXT) tablearray.get(i)).getCset());
            }

            // Step 4 search
            Server_JDXT server_jdxt = new Server_JDXT(tset, f_array, cset_array);
            long searchprepare_end = System.nanoTime();

            long server_start = System.nanoTime();
            ArrayList<ArrayList<ArrayList<byte[]>>> encRes = server_jdxt.search(joinquery.getStokenList(), joinquery.getGgmxTokenList());
            long server_end = System.nanoTime();
            if (x == 0)
                System.out.println("encRes size: " + encRes.size());
            long decrypt_start = System.nanoTime();
            ArrayList<ArrayList<ArrayList<String>>> res = joinquery.JDXT_get_res(encRes);
            long decrypt_end = System.nanoTime();

            long search_end = System.nanoTime();
            search_all += search_end - search_start;
            decrypt_all += decrypt_end - decrypt_start;
            server_all += server_end - server_start;
            searchprepare_all += searchprepare_end - search_start;
            tokenprior_all += tokenprior_end - search_start;
            if (x == 0) {
                for (int i = 0; i < res.size(); i++) {
                    System.out.println("----------------" + i + "----------------");
                    for (int j = 0; j < res.get(i).size(); j++) {
                        System.out.println(i + ": " + j + "table result:");
                        for (int k = 0; k < res.get(i).get(j).size(); k++)
                            System.out.print(res.get(i).get(j).get(k) + ", ");
                        System.out.println();
                    }
                }
            }
        }
        System.out.println("JDXT average search time : " + search_all / Math.pow(10, 6 + 2) + " ms");
        System.out.println("JDXT average decrypt time : " + decrypt_all / Math.pow(10, 6 + 2) + " ms");
        System.out.println("JDXT average server time : " + server_all / Math.pow(10, 6 + 2) + " ms");
        System.out.println("JDXT average search_prepare time : " + searchprepare_all / Math.pow(10, 6 + 2) + " ms");
        System.out.println("JDXT average token and prior time : " + tokenprior_all / Math.pow(10, 6 + 2) + " ms");

    }
}
