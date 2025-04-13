package utils;

import java.util.ArrayList;

public class GGMXTokenList {
    public ArrayList<byte[]> attrList;//F(k_x, attr)
    public GGMNode[] w_cnt_List;//F(k_z, w1||i) for ggmTree
    public ArrayList<GGMNode[]> w_jk_List;//F(k_z1, wj||k) for ggmTree
    public GGMXTokenList(ArrayList<byte[]> attrList, GGMNode[] w_cnt_List, ArrayList<GGMNode[]> w_jk_List){
        this.attrList = attrList;
        this.w_cnt_List = w_cnt_List;
        this.w_jk_List = w_jk_List;
    }
    public GGMXTokenList(ArrayList<byte[]> attrList, GGMNode[] w_cnt_List){
        this.attrList = attrList;
        this.w_cnt_List = w_cnt_List;
    }
}
