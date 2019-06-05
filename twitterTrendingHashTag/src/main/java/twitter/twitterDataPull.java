package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import java.io.*;
import java.net.URI;

public class twitterDataPull {
    static long start = System.currentTimeMillis();
    static int createState = 0;
    static long end = start + 2*60*1000;

    public static void dataPull(final String  dst, String[] search){
        final Path path = new Path(dst);
        StatusListener listener = new StatusListener() {

            @Override
            public void onScrubGeo(long l, long l1) {

            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {

            }

            public void onStatus(Status status) {
                try {
                    twitterDataPull.printMethod(status, dst, path);

                } catch (IOException e) {

                }
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            }

            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("u4ffZeRplaEGpZ94Pk9I6CbPp")
                .setOAuthConsumerSecret("SUCMOFhcGWfyIklPAqlPyzLqQuz8aanEANzDm6Mek4o591mitC")
                .setOAuthAccessToken("1092853592644571136-8MKPInQrqrMFed1qPAHwaHUV0ASKp7")
                .setOAuthAccessTokenSecret("teVEchbsYrwpke6BSWQg1Vmoi6zDb8GxFMXFDi6H7Eikz");
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        twitterStream.addListener(listener);

        FilterQuery filter = new FilterQuery();
        if(search != null) {
            //String[] keywordsArray = search};
            filter.track(search);
            twitterStream.filter(filter);
        }
    }

    public static void printMethod(Status status, String dst, Path path) throws IOException{


        if(!status.isRetweet()) {
            InputStream in = new BufferedInputStream(new ByteArrayInputStream((status.getText()+"\n").getBytes()));
            System.out.println(status.getUser().getName() + " : " + status.getText());
            System.out.println(status.getCreatedAt() + " " + status.getUser().getScreenName());
            System.out.println(createState + "-------" + System.currentTimeMillis() + "--------" + end + "------------------------------");
            System.out.println((System.currentTimeMillis() > end) + "---------------------------------------------");
            Configuration conf = new Configuration();
            conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
            conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));


            FileSystem fs = FileSystem.get(URI.create(dst), conf);
            OutputStream out;

            if(createState == 0) {
                out = fs.create(path);
                createState = 1;
            }
            else {
                out = fs.append(path);
            }
            IOUtils.copyBytes(in, out, 4096, true);
            System.out.println("Before condition " + (System.currentTimeMillis() > end));
            if(System.currentTimeMillis() > end){
                try {
                    hashTagFilter.countHashTag("hdfs://cshadoop1.utdallas.edu/user/nxm180018/bigdata/");
                }catch (Exception e){
                }
                out.close();
                System.exit(0);
                System.out.println("IN condition"+(System.currentTimeMillis() > end));
            }
            System.out.println("after condition"+(System.currentTimeMillis() > end));
        }

    }


}
