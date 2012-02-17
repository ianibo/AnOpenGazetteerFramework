#!/usr/bin/groovy

@GrabResolver(name='es', root='https://oss.sonatype.org/content/repositories/releases')
// @Grab(group='com.gmongo', module='gmongo', version='0.9.2')
@Grapes([
  @Grab(group='net.sf.opencsv', module='opencsv', version='2.0'),
//   @Grab(group='org.elasticsearch', module='elasticsearch', version='0.19.0.RC2'),
  @Grab(group='org.elasticsearch', module='elasticsearch-lang-groovy', version='1.1.0')])

import au.com.bytecode.opencsv.CSVReader
import org.elasticsearch.groovy.node.GNode
import org.elasticsearch.groovy.node.GNodeBuilder
import org.apache.log4j.*
import static org.elasticsearch.groovy.node.GNodeBuilder.*

LogManager.rootLogger.level = Level.DEBUG

println("Initialise");
org.elasticsearch.groovy.node.GNode esnode = init();
org.elasticsearch.groovy.client.GClient esclient = esnode.getClient()

// Load file
def FILENAME="uk_gaz_with_geo_no_tfr.csv"
CSVReader r = new CSVReader( new InputStreamReader(getClass().classLoader.getResourceAsStream(FILENAME)))

// String [] nl;
// while ((nl = r.readNext()) != null) {
  // authority_id,id,type,place_name,fqn,alias,centroid_lat,centroid_lon
//   println "Processing authority:${nl[0]} identifier:${nl[1]} type:${nl[2]} place_name:${nl[3]} fqn:${nl[4]} alias:${nl[5]} lat:${nl[6]} lon:${nl[7]}"
//   writeGazRecord(esclient, nl[0],nl[1],nl[2],nl[3],nl[4],nl[5],nl[6],nl[7]);
// }

esnode.stop().close();

println("All done");

System.exit(0);


// N.B. Prefixed parameters wit p_ so they don't clash with variables from the es index closure. Causes problems if they do!
def writeGazRecord(esclient, p_authority_id,p_id,p_rtype,p_place_name,p_fqn,p_alias,p_centroid_lat,p_centroid_lon) {

  def rectype = "unknown"
  switch ( p_rtype ) {
    case "1. postcode":
      rectype = 'postcode';
      break;
    case "3. Locality":
      rectype = 'locality';
      break;
  }
  
  def place_pojo = [
        "id":"${p_id}".toString(),
        "type":"${p_rtype}".toString(),
        "authority":"${p_authority_id}".toString(),
        "placeName":"${p_place_name}".toString(),
        "fqn":"${p_fqn}".toString(),
        "aliases":["${p_alias}".toString()],
        "lat":"${p_centroid_lat}".toString(),
        "lon":"${p_centroid_lon}".toString()
  ];

  println("Indexing record of type ${rectype} with ID ${p_id}");

  try {
    def future = esclient.index {
      index "gaz"
      type "${rectype}"
      // id id.toString()
      source place_pojo
    }
    println("Indexed respidx:$future.response.index / resptp:$future.response.type / respid:$future.response.id")
  }
  catch ( Exception e ) {
    println("Problem: ${e}");
    e.printStackTrace();
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
    network {
      host = "_eth0_"
    }
  }


  println("Constructing node...");
  def nb = nodeBuilder.node()

  println("Got node: ${nb}");

  return nb;
}

def setup(esclient) {

    org.elasticsearch.groovy.client.GIndicesAdminClient index_admin_client = new org.elasticsearch.groovy.client.GIndicesAdminClient(esclient);
    def future = index_admin_client.putMapping {
      indices 'gaz'
      type 'postcode'
      source {
        postcode {       // Think this is the name of the mapping within the type
          properties {
            title { // We declare a multi_field mapping so we can have a default "title" search with stemming, and an untouched title via origtitle
              type = 'multi_field'
              fields {
                title {
                  type = 'string'
                  analyzer = 'snowball'
                }
                origtitle {
                  type = 'string'
                  store = 'yes'
                }
              }
            }
            subject {
              type = 'multi_field'
              fields {
                subject {
                  type = 'string'
                  store = 'yes'
                  index = 'not_analyzed'
                }
                subjectKw {
                  type = 'string'
                  analyzer = 'snowball'
                }
              }

            }
            provid {
              type = 'string'
              store = 'yes'
              index = 'not_analyzed'
            }
          }
        }
      }
    }
}
