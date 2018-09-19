package mx.iteso.desi.cloud.lp1;

import java.io.IOException;
import mx.iteso.desi.cloud.keyvalue.IKeyValueStorage;
import mx.iteso.desi.cloud.keyvalue.KeyValueStoreFactory;
import mx.iteso.desi.cloud.keyvalue.ParseTriples;
import mx.iteso.desi.cloud.keyvalue.PorterStemmer;
import mx.iteso.desi.cloud.keyvalue.Triple;

public class IndexImages {
  ParseTriples parser;
  IKeyValueStorage imageStore, titleStore;
    
  public IndexImages(IKeyValueStorage imageStore, IKeyValueStorage titleStore) {
	  this.imageStore = imageStore;
	  this.titleStore = titleStore;
  }
      
  public void run(String imageFileName, String titleFileName) throws IOException
  {
    //parse all the images to triples
    parser = new ParseTriples(imageFileName);

    //get first triple
    Triple imageTriple;
    int inc1 = 0, inc2 = 0;

    while((imageTriple = parser.getNextTriple()) != null){
      if(imageTriple.getObject().contains("FilePath/" + Config.filter) && 
        imageTriple.getPredicate().contains("http://xmlns.com/foaf/0.1/depiction")){
          //in triple, A = subject, B = predicate, C = object. So get A and C.
          imageStore.addToSet(imageTriple.getSubject(), imageTriple.getObject());
          inc1++;
      }
    };

    parser.close();

    //now do the same, but for l
    parser = new ParseTriples(titleFileName);

    Triple titleTriple;
    while((titleTriple = parser.getNextTriple()) != null){
      
      //check that there is an image for current label.
      if(imageStore.exists(titleTriple.getSubject()) && 
        titleTriple.getPredicate().contains("http://www.w3.org/2000/01/rdf-schema#label")){
        for (String singleLabel : titleTriple.getObject().split(" ")){
          String keyword = PorterStemmer.stem(singleLabel.toLowerCase());
          if(!keyword.equals("Invalid term") && !keyword.equals("No term entered") && 
            !(keyword.length() == 0)){
            titleStore.addToSet(keyword, titleTriple.getSubject());
            inc2++;
          }
        }
      }
      //get first triple
      titleTriple = parser.getNextTriple();
    };

    parser.close();
    System.out.println("imagesnes: " + inc1 + "terms: " + inc2);
  }
  
  public void close() {
    imageStore.close();
    titleStore.close();
  }
  
  public static void main(String args[])
  {
    System.out.println("*** Alumno: _____________________ (Exp: _________ )");
    try {

      IKeyValueStorage imageStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType, 
    		"images");
      IKeyValueStorage titleStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType, 
  			"terms");

      IndexImages indexer = new IndexImages(imageStore, titleStore);
      indexer.run(Config.imageFileName, Config.titleFileName);
      System.out.println("Indexing completed");
      indexer.close();
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Failed to complete the indexing pass -- exiting");
    }
  }
}

