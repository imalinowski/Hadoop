import java.io.IOException;
import java.io.StringReader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
    private StandardAnalyzer analyzer;
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void setup(Context context) {
        analyzer = new StandardAnalyzer();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try (TokenStream tokenStream = analyzer.tokenStream(null, new StringReader(value.toString()))) {
            CharTermAttribute attr = tokenStream.addAttribute(CharTermAttribute.class);
            tokenStream.reset();

            while (tokenStream.incrementToken()) {
                String token = attr.toString();
                context.write(new Text(token), one);
            }

            tokenStream.end();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException {
        analyzer.close();
    }
}
