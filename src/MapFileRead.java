import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.IOUtils;

public class MapFileRead {
  public static void main(String[] args) throws IOException {
    String uri = args[0];
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri),conf);
    Path path = new Path(uri); 
    MapFile.Reader reader = null;
    long position = 0;
    try {
      reader = new MapFile.Reader(fs, uri, conf);
      WritableComparable key = (WritableComparable) ReflectionUtils.newInstance(reader.getKeyClass(),conf);
      Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(),conf); 
      while(reader.next(key,value)) {
        String syncSeen = "*";//reader.syncSeen() ? "*" : "";
        System.out.printf("[%s%s]\t%s\t%s\n",position,syncSeen,key,value);
        position++;
      }
     // Text key = new Text("http://my.pconline.com.cn/20379577");
      reader.get(key, value);
      //System.out.printf("%s\n",value.toString());
     } 
      finally {
        IOUtils.closeStream(reader);
	System.out.printf("%s\n",position);
      } 
  }
}


