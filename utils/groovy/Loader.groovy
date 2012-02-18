#!/usr/bin/groovy

import au.com.bytecode.opencsv.CSVReader
import org.elasticsearch.groovy.client.GClient
import org.elasticsearch.groovy.node.GNode
import org.apache.log4j.*


public class Loader {

  public load(filename) {
    println("Initialise");
    GNode esnode = inites();
    GClient esclient = esnode.getClient();

    CSVReader r = new CSVReader( new InputStreamReader(getClass().classLoader.getResourceAsStream(filename)))

    String [] nl;
    while ((nl = r.readNext()) != null) {
      // authority_id,id,type,place_name,fqn,alias,centroid_lat,centroid_lon
      println "Processing authority:${nl[0]} identifier:${nl[1]} type:${nl[2]} place_name:${nl[3]} fqn:${nl[4]} alias:${nl[5]} lat:${nl[6]} lon:${nl[7]}"
      writeGazRecord(esclient, nl[0],nl[1],nl[2],nl[3],nl[4],nl[5],nl[6],nl[7]);
    }

    esnode.stop().close();
  
    println("All done");
  }

  
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
  
  def inites() {
    println("Init");
  
      org.elasticsearch.groovy.node.GNodeBuilder nodeBuilder = new org.elasticsearch.groovy.node.GNodeBuilder()
  
      println("Construct node settings");
  
      org.elasticsearch.groovy.common.xcontent.GXContentBuilder gxc = new org.elasticsearch.groovy.common.xcontent.GXContentBuilder();
  
      def s = gxc.buildAsString {
        rootprop='something'
      }
      println "s=${s}";
  
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
      }
  
      println("Constructing node...${nodeBuilder.getSettings().dump()}");
  
    nodeBuilder.node()
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

}
