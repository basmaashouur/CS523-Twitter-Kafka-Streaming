package cs523.BDTFinalProject;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import cs523.BDTFinalProject.SentimentAnalyzer;
public class KafkaProducer {
    public static void main(String[] args) throws Exception {
        final LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(
                1000);
        String consumerKey = "114mPxt11TJFH2cY2p6lPULm5";
        String consumerSecret = "yn3alzOxaLyt9wM6B3AMn9df1fP0mnRzOFkmJnQ7VxldslBlQQ";
        String accessToken = "1022211797544263681-o0NfKGYQwyhF2kHT6Ec7y5Ia59h9dA";
        String accessTokenSecret = "ofUc1OUziDK6Xi0y2FtPsQtA640S9EL8axhRewoodOcTn";
        String topicName = "TwitterDataAnalysisProject";
        SentimentAnalyzer analyzer = new SentimentAnalyzer();
        String[] filterWords = { "ai", "AI", "crypto", "Crypto", "ML", "ml",
                "Machine Learning", "Artificial Intelligence" };
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
                .setOAuthConsumerSecret(consumerSecret)
                .setOAuthAccessToken(accessToken)
                .setOAuthAccessTokenSecret(accessTokenSecret);
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build())
                .getInstance();
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                queue.offer(status);
            }
            @Override
            public void onDeletionNotice(
                    StatusDeletionNotice statusDeletionNotice) {
                System.out.println("deletion notice");
            }
            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("track limitation notice");
            }
            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("scrub_geo event");
            }
            @Override
            public void onStallWarning(StallWarning warning) {
                System.out.println("stall warning");
            }
            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        twitterStream.addListener(listener);
        FilterQuery query = new FilterQuery().track(filterWords);
        twitterStream.filter(query);
        Properties properties = new Properties();
        properties.put("metadata.broker.list", "localhost:9092");
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerialize" + "r");
        @SuppressWarnings("resource")
        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(
                properties);
        int j = 0;
        while (true) {
            Status ret = queue.poll();
            if (ret == null) {
                Thread.sleep(100);
                // i++;
            } else {
                System.out.println("Tweet:" + ret);
                String sentiment = analyzer.findSentiment(ret.getText());
                HashtagEntity[] entities = ret.getHashtagEntities();
                String hashtags = "";
                if(entities.length != 0){
                    Pattern p = Pattern.compile("\"([^\"]*)\"");
                    Matcher m = p.matcher(entities[0].toString());
                    while (m.find()) {
                        hashtags = m.group(1) + "|";
                    }
                    for (int i = 1; i < entities.length -1 ; i++) {
                        Pattern p1 = Pattern.compile("\'([^\']*)\'");
                        Matcher m1 = p1.matcher(entities[i].toString());
                        while (m1.find()) {
                            hashtags += m1.group(1) + "|";
                        }
                    }
                }                
                @SuppressWarnings("deprecation")
                String msg = new String(ret.getCreatedAt() + ", "
                        + ret.getId() + ", "
                        + ret.getUser().getId() + ", "
                        + getLocation(ret.getUser().getLocation()) + ", "
                        + ret.getUser().getFollowersCount() + ", "
                        + ret.getUser().isVerified() + ", "
                        + ret.getUser().getCreatedAt() + ", "
                        + ret.getUser().getTimeZone() + ", " 
                        + sentiment + ", " + 
                        + ret.getCreatedAt().getHours() + ", "
                        + ret.getCreatedAt().getMinutes() + ", "
                        + ret.getCreatedAt().getSeconds() + ", "
                        + ret.getUser().getCreatedAt().getMonth() + ", "
                        + ret.getUser().getCreatedAt().getYear() + ", "
                        + hashtags + ", "
                        + ret.getId() + ", " + ret.getUser().getName() + ", "
                        + ret.getRetweetCount() + ", " + ret.getText())
                ;
                producer.send(new ProducerRecord<String, String>(topicName,
                        Integer.toString(j++), msg));
            }
        }
    }
    private static String getLocation(String loc) {
        if (loc == null)
            return "null";
        else
            return loc.split(",")[0];
    }
}