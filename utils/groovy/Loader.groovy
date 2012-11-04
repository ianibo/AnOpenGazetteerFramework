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

    // To clear down the gaz: curl -XDELETE 'http://localhost:9200/gaz'

    setup(esclient);

    CSVReader r = new CSVReader( new InputStreamReader(getClass().classLoader.getResourceAsStream(filename)))

    String [] nl;
    while ((nl = r.readNext()) != null) {
      // authority_id,id,type,place_name,fqn,alias,centroid_lat,centroid_lon
      // authority_id,id,type,place_name,display,fqn,alias,centroid_lat,centroid_lon
      println "Processing authority:${nl[0]} identifier:${nl[1]} type:${nl[2]} place_name:${nl[3]} display:${nl[4]} fqn:${nl[5]} alias:${nl[6]} lat:${nl[7]} lon:${nl[8]}"
      writeGazRecord(esclient, nl[0],nl[1],nl[2],nl[3],nl[4],nl[5],nl[6],nl[7],nl[8]);
    }

    esnode.stop().close();
  
    println("All done");
  }

  
  // N.B. Prefixed parameters wit p_ so they don't clash with variables from the es index closure. Causes problems if they do!
  def writeGazRecord(esclient, p_authority_id,p_id,p_rtype,p_place_name,p_dispname, p_fqn,p_alias,p_centroid_lat,p_centroid_lon) {
  
    def rectype = "unknown"
    def pref = 5
    switch ( p_rtype ) {
      case "1. postcode":
        rectype = 'postcode';
        pref=1
        break;
      case "2. Thoroughfare":
        rectype = 'thoroughfare';
        pref=2
        break;
      case "3. Locality":
        rectype = 'locality';
        pref=3
        break;
      case "4. PostTown":
        rectype = 'posttown';
        pref=4
        break;
    }

    double lat = Double.parseDouble(p_centroid_lat);
    double lon = Double.parseDouble(p_centroid_lon);
    
    def place_pojo = [
          "id":"${p_id}".toString(),
          "type":"${rectype}".toString(),
          "pref":"${pref}".toString(),
          "authority":"${p_authority_id}".toString(),
          "placeName":"${p_place_name}".toString(),
          "alias":"${p_alias}".toString(),
          "fqn":"${p_dispname}".toString(),
          "timestamp":System.currentTimeMillis(),
          // "prefixfqn":"${p_fqn}".toString(),
          // "aliases":["${p_alias}".toString()],
          location : [
            "lat":p_centroid_lat,
            "lon":p_centroid_lon
          ]
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
  
      nodeBuilder.
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
      println("Writing mappings.. this will list mappings in browser: http://localhost:9200/gaz/_mapping?pretty=true");

      println("Create gaz index");
      org.elasticsearch.groovy.client.GIndicesAdminClient index_admin_client = new org.elasticsearch.groovy.client.GIndicesAdminClient(esclient);
      def future = index_admin_client.create {
        index 'gaz'
      }

      println("Register mappings");

      future = index_admin_client.putMapping {
        indices 'gaz'
        type 'postcode'
        source {
          gazmap {
            properties {
              pref {
                type = 'string';
              }
              fqn {
                type = 'multi_field'
                fields {
                  orig {
                    type = 'string'
                    store = 'yes'
                    index = 'not_analyzed'
                  }
                  fqn {
                    type = 'string'
                    // analyzer = 'snowball'
                  }
                }
              }
              location {
                type='geo_point'
              }
            }
          }
        }
      }
      println("Installed postcode mappings");

      future = index_admin_client.putMapping {
        indices 'gaz'
        type 'thoroughfare'
        source {
          gazmap {
            properties {
              pref {
                type = 'string';
              }
              fqn {
                type = 'multi_field'
                fields {
                  orig {
                    type = 'string'
                    store = 'yes'
                    index = 'not_analyzed'
                  }
                  fqn {
                    type = 'string'
                    // analyzer = 'snowball'
                  }
                }
              }
              location {
                type='geo_point'
              }
            }
          }
        }
      }
      println("Installed tfr mappings");

      future = index_admin_client.putMapping {
        indices 'gaz'
        type 'locality'
        source {
          gazmap {
            properties {
              pref {
                type = 'string';
              }
              fqn {
                type = 'multi_field'
                fields {
                  orig {
                    type = 'string'
                    store = 'yes'
                    index = 'not_analyzed'
                  }
                  fqn {
                    type = 'string'
                    // analyzer = 'snowball'
                  }
                }
              }
              location {
                type='geo_point'
              }
            }
          }
        }
      }
      println("Installed locality mappings");

      future = index_admin_client.putMapping {
        indices 'gaz'
        type 'posttown'
        source {
          gazmap {
            properties {
              pref {
                type = 'string';
              }
              fqn {
                type = 'multi_field'
                fields {
                  orig {
                    type = 'string'
                    store = 'yes'
                    index = 'not_analyzed'
                  }
                  fqn {
                    type = 'string'
                    // analyzer = 'snowball'
                  }
                }
              }
              location {
                type='geo_point'
              }
            }
          }
        }
      }
      println("Installed town mappings");


    println("Mappings installed, wait 10");
    synchronized(this) {
      this.sleep(10000);
    }
  }

}
