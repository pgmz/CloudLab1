package mx.iteso.desi.cloud.lp1;

import java.util.Set;
import java.util.Iterator;
import java.util.HashSet;
import mx.iteso.desi.cloud.keyvalue.KeyValueStoreFactory;
import mx.iteso.desi.cloud.keyvalue.IKeyValueStorage;
import mx.iteso.desi.cloud.keyvalue.PorterStemmer;

public class QueryImages {
  IKeyValueStorage imageStore;
  IKeyValueStorage titleStore;
	
  public QueryImages(IKeyValueStorage imageStore, IKeyValueStorage titleStore) 
  {
	  this.imageStore = imageStore;
	  this.titleStore = titleStore;
  }
	
  public Set<String> query(String word)
  {

    //stem the key to find
    String stemWord = PorterStemmer.stem(word);
    HashSet<String> set = new HashSet<>();

    for (String tag : titleStore.get(stemWord)){
      for (String url : imageStore.get(tag)){
        set.add(url);
      }
    }

    return set;
  }
        
  public void close()
  {
    imageStore.close();
    titleStore.close();
  }
	
  public static void main(String args[]) 
  {
    System.out.println("*** Alumno: _____________________ (Exp: _________ )");
    
    try{
      IKeyValueStorage imageStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType, "images");
      IKeyValueStorage titleStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType, "terms");

      QueryImages myQuery = new QueryImages(imageStore, titleStore);

      for (int i=0; i<args.length; i++) {
        System.out.println(args[i]+":");
        Set<String> result = myQuery.query(args[i]);
        Iterator<String> iter = result.iterator();
        while (iter.hasNext()) 
          System.out.println("  - "+iter.next());
      }
      
      myQuery.close();

    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Failed to complete the indexing pass -- exiting");
    }
  }
}

