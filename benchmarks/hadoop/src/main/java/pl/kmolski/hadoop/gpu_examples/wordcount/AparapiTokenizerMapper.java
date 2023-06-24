package pl.kmolski.hadoop.gpu_examples.wordcount;

import com.aparapi.Kernel;
import com.aparapi.Range;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.stream.IntStream;

public class AparapiTokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    abstract static class WordSearchKernel extends Kernel {

        protected boolean isDelimiter(byte character) {
            return character == ' '
                    || character == '\t'
                    || character == '\n'
                    || character == '\r'
                    || character == '\f';
        }
    }

    private static final IntWritable one = new IntWritable(1);

    private boolean isDelimiter(byte character) {
        return character == ' '
            || character == '\t'
            || character == '\n'
            || character == '\r'
            || character == '\f';
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        System.setProperty("com.aparapi.logLevel", "FINE");
        final byte[] chunk = value.getBytes();
        final int chunkLen = value.getLength();

        final int[] wordStart = IntStream.range(0, chunkLen).parallel()
                .filter(i -> {
                    final boolean currentIsDelimiter = isDelimiter(chunk[i]);
                    if (i == 0) {
                        return !currentIsDelimiter;
                    } else {
                        final boolean prevIsDelimiter = isDelimiter(chunk[i - 1]);
                        return prevIsDelimiter && !currentIsDelimiter;
                    }
                }).toArray();
        final int[] wordLength = new int[wordStart.length];
        Kernel findWordEnds = new WordSearchKernel() {

            @Override
            public void run() {
                final int w = getGlobalId();
                final int start = wordStart[w];
                final int end = ((w != getGlobalSize() - 1) ? wordStart[w + 1] : chunkLen) - 1;

                wordLength[w] = end - start + 1;
                for (int i = end - 1; i >= start; --i) {
                    final boolean currentIsDelimiter = isDelimiter(chunk[i]);
                    final boolean nextIsDelimiter = isDelimiter(chunk[i + 1]);

                    if (!currentIsDelimiter && nextIsDelimiter) {
                        wordLength[w] = i - start + 1;
                    }
                }
            }
        };
        findWordEnds.execute(Range.create(wordStart.length));
        findWordEnds.dispose();

        Text word = new Text();
        for (int i = 0; i < wordStart.length; i++) {
            word.set(chunk, wordStart[i], wordLength[i]);
            context.write(word, one);
        }
    }
}
