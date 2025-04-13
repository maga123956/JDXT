import client.Setup_JDXT;
import client.Setup_JDXTEMM;
import server.Server_JDXT;
import server.Server_JDXTEMM;
import utils.Bloom;
import utils.Jointuple;
import utils.query;

import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Map;

public class JDXTEMM {
    public static void main(String[] args) throws NoSuchAlgorithmException, InvalidKeyException {
        int table_num = 3;
        int join_column = 2;
        ArrayList<Object> tablearray = new ArrayList<>();
        for (int i = 0; i < table_num; i++) {
            Setup_JDXTEMM tab = new Setup_JDXTEMM(join_column);
            tablearray.add(tab);
        }
        ArrayList<Jointuple> joinArray1 = new ArrayList<>();
        for (int i = 0; i < join_column; i++) {
            // attr_0 w_0 attr_1 w_1
            Jointuple tuple = new Jointuple("attr_attr_" + i, "w_w_" + i);
            joinArray1.add(tuple);
        }
        query query11 = new query("ind1", "w1", "add", joinArray1);
        ((Setup_JDXTEMM)tablearray.get(0)).JDXTEMM_update(query11);

        query query12 = new query("ind1", "w1", "del", joinArray1);
        ((Setup_JDXTEMM)tablearray.get(0)).JDXTEMM_update(query12);


        query query13 = new query("ind3", "w1", "add", joinArray1);
        ((Setup_JDXTEMM)tablearray.get(0)).JDXTEMM_update(query13);


        ArrayList<Jointuple> joinArray2 = new ArrayList<>();
        for (int i = 0; i < join_column; i++) {
            // attr_0 w_0 attr_1 w_0
            Jointuple tuple = new Jointuple("attr_attr_" + i, "w_w_" + 0);
            joinArray2.add(tuple);
        }
        query query21 = new query("ind1", "w2", "add", joinArray2);
        ((Setup_JDXTEMM)tablearray.get(1)).JDXTEMM_update(query21);



        query query22 = new query("ind2", "w2", "add", joinArray2);
        ((Setup_JDXTEMM)tablearray.get(1)).JDXTEMM_update(query22);


        query query23 = new query("ind3", "w4", "add", joinArray2);
        ((Setup_JDXTEMM)tablearray.get(1)).JDXTEMM_update(query23);


        ArrayList<Jointuple> joinArray3 = new ArrayList<>();
        for (int i = 0; i < join_column; i++) {
            // attr_0 w_0 attr_1 w_1
            Jointuple tuple = new Jointuple("attr_attr_" + i, "w_w_" + i);
            joinArray3.add(tuple);
        }
        query query31 = new query("ind1", "w3", "add", joinArray3);
        ((Setup_JDXTEMM)tablearray.get(2)).JDXTEMM_update(query31);


        query query32 = new query("ind2", "w3", "add", joinArray3);
        ((Setup_JDXTEMM)tablearray.get(2)).JDXTEMM_update(query32);


        query query33 = new query("ind1", "w3", "del", joinArray3);
        ((Setup_JDXTEMM)tablearray.get(2)).JDXTEMM_update(query33);


        // join
        ArrayList<String> val_array = new ArrayList<>();
        ArrayList<String> attr_array = new ArrayList<>();
        for (int i = 0; i < table_num; i++) {
            val_array.add("w" + (i + 1));
            if (i != 1)
                attr_array.add("attr_attr_0");
            else
                attr_array.add("attr_attr_1");
        }
        System.out.println(val_array);
        System.out.println(attr_array);
        query joinquery = new query(tablearray, val_array, attr_array);
        joinquery.JDXTEMM_getJoinToken();

        int prior_pos = joinquery.getPrior_pos();
        System.out.println("prior_pos : " + prior_pos);
        Map<BigInteger, byte[]> tset = ((Setup_JDXTEMM)tablearray.get(prior_pos)).getTset();
        ArrayList<Bloom> f_array = new ArrayList<>();
        ArrayList<Map<BigInteger, ArrayList<byte[]>>> cset_array = new ArrayList<>();
        for (int i = 0; i < table_num; i++) {
            f_array.add(((Setup_JDXTEMM)tablearray.get(i)).getF());
            cset_array.add(((Setup_JDXTEMM)tablearray.get(i)).getCset());
        }

        Server_JDXTEMM server_jdxtemm = new Server_JDXTEMM(tset, f_array, cset_array);
        ArrayList<ArrayList<ArrayList<byte[]>>> encRes = server_jdxtemm.search(joinquery.getStokenList(), joinquery.getGgmxTokenList());
        System.out.println("encRes size: " + encRes.size());
        ArrayList<ArrayList<ArrayList<String>>> res = joinquery.JDXT_filter_res(encRes);
        System.out.println("res size: " + res.size());

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
