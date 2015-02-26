import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public  class ReduceInvertedIndex extends Reducer<Text, Text, Text, Text> {
private Text docIds = new Text();
@Override
public void reduce(Text key, Iterable<Text> values, Context context)
throws IOException, InterruptedException {
HashSet<String> uniqueDocIds = new HashSet<String>();
for (Text docId : values) {
uniqueDocIds.add(docId.toString());
}
docIds.set(new Text(StringUtils.join(uniqueDocIds, ",")));
context.write(key, docIds);
}
}
