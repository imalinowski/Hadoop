import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> countMap = new HashMap<>();

        for (Text val : values) {
            String nextWord = val.toString();
            countMap.put(nextWord, countMap.getOrDefault(nextWord, 0) + 1);
        }

        String mostFrequentWord = null;
        int maxCount = 0;
        for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
            if (entry.getValue() > maxCount) {
                mostFrequentWord = entry.getKey();
                maxCount = entry.getValue();
            }
        }

        if (mostFrequentWord != null) {
            context.write(key, new Text(mostFrequentWord));
        }
    }
}
