package utils;

import java.util.ArrayList;

public class Restuple {
    public ArrayList<ArrayList<ArrayList<int[]>>> encRes;
    public ArrayList<ArrayList<ArrayList<byte[]>>> xtag_array;
    public Restuple(ArrayList<ArrayList<ArrayList<int[]>>> encRes, ArrayList<ArrayList<ArrayList<byte[]>>> xtag_array){
        this.encRes = encRes;
        this.xtag_array = xtag_array;
    }
}
