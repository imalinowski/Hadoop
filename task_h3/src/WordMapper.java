import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

public class WordMapper extends Mapper<Object, Text, Text, Text> {
    private StandardAnalyzer analyzer;

    @Override
    protected void setup(Context context) {
        analyzer = new StandardAnalyzer();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        List<String> words = new ArrayList<>();

        try (TokenStream tokenStream = analyzer.tokenStream(null, new StringReader(value.toString()))) {
            CharTermAttribute attr = tokenStream.addAttribute(CharTermAttribute.class);
            OffsetAttribute offsetAttr = tokenStream.addAttribute(OffsetAttribute.class);

            tokenStream.reset();
            int lastOffset = -1;

            while (tokenStream.incrementToken()) {
                int currentOffset = offsetAttr.startOffset();

                if (lastOffset != -1 && lastOffset + 1 < currentOffset) {
                    // Разрыв между словами (скорее всего из-за знаков препинания окончания предложения)
                    words.add(null); // Маркер конца предложения
                }

                words.add(attr.toString());
                lastOffset = offsetAttr.endOffset();
            }

            tokenStream.end();
        }

        // Теперь пробегаем по списку и формируем пары
        for (int i = 0; i < words.size() - 1; i++) {
            String current = words.get(i);
            String next = words.get(i + 1);

            if (current != null && next != null) {
                context.write(new Text(current), new Text(next));
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException {
        analyzer.close();
    }
}