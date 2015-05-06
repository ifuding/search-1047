package SearchPackage;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.IOUtils;

public class MapFileRead {
  
  private static MapFile.Reader reader = null;
  private static Configuration conf = null;
  private static FileSystem fs = null;
  private static Path path = null;
    
  public MapFileRead(String uri) throws IOException{
    this.conf = new Configuration();
    this.fs = FileSystem.get(URI.create(uri), conf);
    this.path = new Path(uri);
    this.reader = new MapFile.Reader(fs, uri, conf);
  }
     
  public static double getValue(String url) throws IOException{
    
    DoubleWritable value = new DoubleWritable(0);

    try {
      reader.get(new Text(url), value);
    }   
    finally {
      if(value == null) {
        return 0;
      }
      return value.get();
    }
  }
  
  public static void main(String[] args) throws IOException {
    
    String uri = args[0];
    conf = new Configuration();
    fs = FileSystem.get(URI.create(uri),conf);
    path = new Path(uri); 
    try {
      reader = new MapFile.Reader(fs, uri, conf);
      WritableComparable key = (WritableComparable) ReflectionUtils.newInstance(reader.getKeyClass(),conf);
      Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(),conf); 
      while(reader.next(key,value)) {
        System.out.printf("%s\t%s\n", key, value);
      }
     } 
      finally {
        IOUtils.closeStream(reader);
      } 
  }
}

