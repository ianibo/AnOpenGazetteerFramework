#!/usr/bin/groovy

@GrabResolver(name='es', root='https://oss.sonatype.org/content/repositories/releases')
// @Grab(group='com.gmongo', module='gmongo', version='0.9.2')
@Grapes([
  @Grab(group='net.sf.opencsv', module='opencsv', version='2.0'),
  @Grab(group='org.elasticsearch', module='elasticsearch-lang-groovy', version='1.0.0')])

import au.com.bytecode.opencsv.CSVReader
 


println("Initialise");
def gNode = init();
// org.elasticsearch.groovy.client.GClient esclient = esnode.getClient()
def esclient = gNode.getClient()

// Load file
def FILENAME="uk_gaz_with_geo_no_tfr.csv"
CSVReader r = new CSVReader( new InputStreamReader(getClass().classLoader.getResourceAsStream(FILENAME)))

String [] nl;
while ((nl = r.readNext()) != null) {
  // authority_id,id,type,place_name,fqn,alias,centroid_lat,centroid_lon
  println "Processing authority:${nl[0]} identifier:${nl[1]} type:${nl[2]} place_name:${nl[3]} fqn:${nl[4]} alias:${nl[5]} lat:${nl[6]} lon:${nl[7]}"
  writeGazRecord(esclient, nl[0],nl[1],nl[2],nl[3],nl[4],nl[5],nl[6],nl[7]);
}

println("All done");

System.exit(0);

def writeGazRecord(esclient, authority_id,id,type,place_name,fqn,alias,centroid_lat,centroid_lon) {

  def rectype = "unknown"
  switch ( type ) {
    case "1. postcode":
      rectype = "postcode";
      break;
    case "3. Locality":
      rectype = "locality";
      break;
  }
  
  def place_pojo = [
        "type":type,
        "authority":authority_id,
        "placeName":place_name,
        "fqn":fqn,
        "aliases":[alias],
        "lat":centroid_lat,
        "lon":centroid_lon
  ];

  println("Indexing record of type ${rectype} with ID ${id}");
  try {
    def future = esclient.index {
      index "gaz"
      type "${type}"
      id "${id}"
      source place_pojo
    }
    println("Indexed respidx:$future.response.index/resptp:$future.response.type/respid:$future.response.id")
  }
  catch ( Exception e ) {
  }
  finally {
  }
}

def init() {
  println("Init");

  def nodeBuilder = new org.elasticsearch.groovy.node.GNodeBuilder()

  println("Construct node settings");

  nodeBuilder.settings {
    node {
      client = true
    }
    cluster {
      name = "aggr"
    }
    http {
      enabled = false
    }
    discovery {
      zen {
        minimum_master_nodes=1
        ping {
          unicast {
            hosts = [ "localhost" ]
          }
        }
      }
    }
  }

  println("Constructing node...");
  nodeBuilder.node()
}
