import client.Setup_JDXT;
import client.Setup_JDXTEMM;
import client.Setup_JDXTHJS;
import utils.Jointuple;
import utils.query;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

public class test_update {
    public static void main(String[] args) throws NoSuchAlgorithmException, InvalidKeyException {
        int data_num = 1000;
        System.out.println("------------- data_num = " + data_num + "-------------");
        int join_column;
        for (join_column = 1; join_column <= 5; join_column++) {
            System.out.println("------------- join_column = " + join_column + "-------------");
            ArrayList<query> queryList = new ArrayList<>();
            ArrayList<Jointuple> joinArray = new ArrayList<>();
            for (int i = 0; i < join_column; i++) {
                Jointuple tuple = new Jointuple("join-attr" + i, "w" + i);
                joinArray.add(tuple);
            }

            for (int i = 0; i < data_num; i++)
                queryList.add(new query("table_id_" + i, "w" + i, "add", joinArray));


            long jdxt_time = 0;
            long jdxtemm_time = 0;
            long jdxthjs_time = 0;
            for (int i = 0; i < 500; i++) {
                Setup_JDXT tab_jdxt = new Setup_JDXT(join_column);
                Setup_JDXTEMM tab_jdxtemm = new Setup_JDXTEMM(join_column);
                Setup_JDXTHJS tab_jdxthjs = new Setup_JDXTHJS(join_column, 1);


                long update_begin = System.nanoTime();
                tab_jdxt.JDXT_update_batch(queryList);
                long update_end = System.nanoTime();
                jdxt_time += (update_end - update_begin);

                update_begin = System.nanoTime();
                tab_jdxtemm.JDXTEMM_update_batch(queryList);
                update_end = System.nanoTime();
                jdxtemm_time += (update_end - update_begin);

                update_begin = System.nanoTime();
                tab_jdxthjs.JDXTHJS_update_batch(queryList);
                update_end = System.nanoTime();
                jdxthjs_time += (update_end - update_begin);
            }
            System.out.println("JDXT update time is :" + jdxt_time / (Math.pow(10, 6 + 2) * 5) + "ms");
            System.out.println("JDXTEMM update time is :" + jdxtemm_time / (Math.pow(10, 6 + 2) * 5) + "ms");
            System.out.println("JDXTHJS update time is :" + jdxthjs_time / (Math.pow(10, 6 + 2) * 5) + "ms");
        }
    }
}
