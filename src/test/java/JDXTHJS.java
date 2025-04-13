import client.Setup_JDXTHJS;
import server.Server_JDXTHJS;
import utils.*;

import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Map;

public class JDXTHJS {
    public static void main(String[] args) throws NoSuchAlgorithmException, InvalidKeyException {
        int table_num = 3;
        int join_column = 2;
        ArrayList<Object> tablearray = new ArrayList<>();
        for (int i = 0; i < table_num; i++) {
            Setup_JDXTHJS tab = new Setup_JDXTHJS(join_column, i + 1);
            tablearray.add(tab);
        }
        ArrayList<Jointuple> joinArray1 = new ArrayList<>();
        for (int i = 0; i < join_column; i++) {
            // attr_0 w_0 attr_1 w_1
            Jointuple tuple = new Jointuple("attr_" + i, "w_" + i);
            joinArray1.add(tuple);
        }
        query query11 = new query("ind1", "w1", "add", joinArray1);
        ((Setup_JDXTHJS) tablearray.get(0)).JDXTHJS_update(query11);

        query query12 = new query("ind1", "w1", "del", joinArray1);
        ((Setup_JDXTHJS) tablearray.get(0)).JDXTHJS_update(query12);


        query query13 = new query("ind2", "w1", "add", joinArray1);
        ((Setup_JDXTHJS) tablearray.get(0)).JDXTHJS_update(query13);

        query query14 = new query("ind3", "w2", "add", joinArray1);
        ((Setup_JDXTHJS) tablearray.get(0)).JDXTHJS_update(query14);

        ArrayList<Jointuple> joinArray12 = new ArrayList<>();
        for (int i = 0; i < join_column; i++) {
            // attr_0 w_1 attr_1 w_1
            Jointuple tuple = new Jointuple("attr_" + i, "w_" + 1);
            joinArray12.add(tuple);
        }
        query query15 = new query("ind4", "w1", "add", joinArray12);
        ((Setup_JDXTHJS) tablearray.get(0)).JDXTHJS_update(query15);

        query query16 = new query("ind5", "w1", "add", joinArray12);
        ((Setup_JDXTHJS) tablearray.get(0)).JDXTHJS_update(query16);

        query query17 = new query("ind6", "w1", "add", joinArray12);
        ((Setup_JDXTHJS) tablearray.get(0)).JDXTHJS_update(query17);

        query query18 = new query("ind7", "w1", "add", joinArray12);
        ((Setup_JDXTHJS) tablearray.get(0)).JDXTHJS_update(query18);
        System.out.println("table1 cnt is :" + ((Setup_JDXTHJS) tablearray.get(0)).getCnt());


        ArrayList<Jointuple> joinArray2 = new ArrayList<>();
        for (int i = 0; i < join_column; i++) {
            // attr_0 w_0 attr_1 w_0
            Jointuple tuple = new Jointuple("attr_" + i, "w_" + 0);
            joinArray2.add(tuple);
        }
        query query21 = new query("ind1", "w2", "add", joinArray2);
        ((Setup_JDXTHJS) tablearray.get(1)).JDXTHJS_update(query21);


        query query22 = new query("ind1", "w2", "del", joinArray2);
        ((Setup_JDXTHJS) tablearray.get(1)).JDXTHJS_update(query22);


        query query23 = new query("ind3", "w2", "add", joinArray2);
        ((Setup_JDXTHJS) tablearray.get(1)).JDXTHJS_update(query23);

        ArrayList<Jointuple> joinArray21 = new ArrayList<>();
        for (int i = 0; i < join_column; i++) {
            // attr_0 w_1 attr_1 w_1
            Jointuple tuple = new Jointuple("attr_" + i, "w_" + 1);
            joinArray21.add(tuple);
        }

        query query24 = new query("ind4", "w3", "add", joinArray21);
        ((Setup_JDXTHJS) tablearray.get(1)).JDXTHJS_update(query24);

        query query25 = new query("ind5", "w2", "add", joinArray21);
        ((Setup_JDXTHJS) tablearray.get(1)).JDXTHJS_update(query25);

        query query26 = new query("ind6", "w2", "add", joinArray21);
        ((Setup_JDXTHJS) tablearray.get(1)).JDXTHJS_update(query26);
        System.out.println("table2 cnt is :" + ((Setup_JDXTHJS) tablearray.get(1)).getCnt());



        ArrayList<Jointuple> joinArray3 = new ArrayList<>();
        for (int i = 0; i < join_column; i++) {
            // attr_0 w_0 attr_1 w_1
            Jointuple tuple = new Jointuple("attr_" + i, "w_" + i);
            joinArray3.add(tuple);
        }
        query query31 = new query("ind1", "w3", "add", joinArray3);
        ((Setup_JDXTHJS) tablearray.get(2)).JDXTHJS_update(query31);


        query query32 = new query("ind2", "w3", "add", joinArray3);
        ((Setup_JDXTHJS) tablearray.get(2)).JDXTHJS_update(query32);


        query query33 = new query("ind1", "w3", "del", joinArray3);
        ((Setup_JDXTHJS) tablearray.get(2)).JDXTHJS_update(query33);

        ArrayList<Jointuple> joinArray31 = new ArrayList<>();
        for (int i = 0; i < join_column; i++) {
            // attr_0 w_1 attr_1 w_1
            Jointuple tuple = new Jointuple("attr_" + i, "w_" + 1);
            joinArray31.add(tuple);
        }
        query query34 = new query("ind3", "w4", "add", joinArray31);
        ((Setup_JDXTHJS) tablearray.get(2)).JDXTHJS_update(query34);

        query query35 = new query("ind4", "w3", "add", joinArray31);
        ((Setup_JDXTHJS) tablearray.get(2)).JDXTHJS_update(query35);

        query query36 = new query("ind5", "w3", "add", joinArray31);
        ((Setup_JDXTHJS) tablearray.get(2)).JDXTHJS_update(query36);
        System.out.println("table3 cnt is :" + ((Setup_JDXTHJS) tablearray.get(2)).getCnt());


        // join
        ArrayList<String> val_array = new ArrayList<>();
        ArrayList<String> attr_array = new ArrayList<>();
        for (int i = 0; i < table_num; i++) {
            val_array.add("w" + (i + 1));
            attr_array.add("attr_0");
        }
        System.out.println(val_array);
        System.out.println(attr_array);
        query joinquery = new query(tablearray, val_array, attr_array);
        joinquery.JDXTHJS_getJoinToken();

        int prior_pos = joinquery.getPrior_pos();
        System.out.println("prior_pos : " + prior_pos);
        Map<BigInteger, byte[]> tset = ((Setup_JDXTHJS) tablearray.get(prior_pos)).getTset();
        ArrayList<Integer> table_EBTList = new ArrayList<>();
        for (int i = 0; i < table_num; i++)
            table_EBTList.add(((Setup_JDXTHJS) tablearray.get(i)).getCnt());

        Server_JDXTHJS server_jdxthjs = new Server_JDXTHJS(tset, table_EBTList);
        Restuple encRes = server_jdxthjs.search(joinquery.getStokenList(), joinquery.getGgmxTokenList());
        ArrayList<ArrayList<ArrayList<String>>> res = joinquery.JDXTHJS_filter_res_parallel(encRes.encRes, encRes.xtag_array);

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
