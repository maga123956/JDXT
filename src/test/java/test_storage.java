import client.*;

public class test_storage {
    public static void main(String[] args) {
        int join_column = 1;
        int key_column = 10 - join_column;
        int record_num = 75536;//55536 65536 75536
        String condition = "_Lmax100";
        String id = "1";

        {
            System.out.println("------------- JDXT begin to setup------------");
            Setup_JDXT[] table_JDXT = new Setup_JDXT[1];
            table_JDXT[0] = new Setup_JDXT(join_column);
            table_JDXT[0].construct(1, key_column, record_num, condition);
            table_JDXT[0].Store(id);
            System.out.println("------------ JDXT setup complete ------------");
        }

        {
            System.out.println("------------- JDXTEMM begin to setup------------");
            Setup_JDXTEMM[] table_JDXTEMM = new Setup_JDXTEMM[1];
            table_JDXTEMM[0] = new Setup_JDXTEMM(join_column);
            table_JDXTEMM[0].construct(1, key_column, record_num, condition);
            table_JDXTEMM[0].Store(id);
            System.out.println("------------ JDXTEMM setup complete ------------");
        }

        {
            System.out.println("------------- JDXTHJS begin to setup------------");
            Setup_JDXTHJS[] table_JDXTHJS = new Setup_JDXTHJS[1];
            table_JDXTHJS[0] = new Setup_JDXTHJS(join_column, 1);
            table_JDXTHJS[0].construct(key_column, record_num, condition);
            table_JDXTHJS[0].Store(id);
            System.out.println("------------ JDXTHJS setup complete ------------");
        }
        System.out.println("The output of EDB is in data/EDB");
    }
}
