package twitter;
import java.util.Arrays;
public class Main {
    public static  void main(String[] args) throws Exception {
        String dst = args[0] + "twitterfeed";
//      for(int i=0; i< args.length; i++){
//        System.out.println(i+args[i]);}

        String[] search = Arrays.copyOfRange(args,1,args.length);
//        for(int i=0; i< search.length; i++) {
//            System.out.println(search[i]);
//        }
            twitterDataPull.dataPull(dst,search);
            hashTagFilter.countHashTag(args[0]);
        }
}
