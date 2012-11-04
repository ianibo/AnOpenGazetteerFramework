#!/usr/bin/groovy

@GrabResolver(name='es', root='https://oss.sonatype.org/content/repositories/releases')
@GrabResolver(name='mygrid', root='http://build.mygrid.org.uk/maven/repository')
// @Grab(group='com.gmongo', module='gmongo', version='0.9.2')
@Grapes([
  @Grab(group='net.sf.opencsv', module='opencsv', version='2.0'),
  @Grab(group='org.elasticsearch', module='elasticsearch', version='0.19.4'),
  @Grab(group='uk.org.mygrid.resources.jcoord', module='jcoord', version='1.0'),
  @Grab(group='org.elasticsearch', module='elasticsearch-lang-groovy', version='1.1.0')])

import org.apache.log4j.*
import uk.me.jstott.jcoord.*;
import au.com.bytecode.opencsv.CSVReader


LogManager.rootLogger.level = Level.DEBUG
log = Logger.getLogger(this.class)

log.debug("ESLoadGaz");

if ( args.length == 0 ) {
  println("Usage: ESLoadGaz <codepoint open csv file>");
}
else {
  log.debug("Starting...");

  GNode esnode = inites();
  GClient esclient = esnode.getClient();

  // To clear down the gaz: curl -XDELETE 'http://localhost:9200/gaz'
  setup(esclient);

  def charset = java.nio.charset.Charset.forName('UTF-8'); // ISO-8859-1
  CSVReader r = new CSVReader( new InputStreamReader(new FileInputStream(args[0]),charset) )
  String[] nl
  while ((nl = r.readNext()) != null) {  
    log.debug("Procesing ${nl}");
    Double e = Double.valueOf(nl[2])
    Double n = Double.valueOf(nl[3])
    OSRef os1 = new OSRef(e.doubleValue(), n.doubleValue());
    LatLng ll1 = os1.toLatLng();
    ll1.toWGS84();
    log.debug("Geocoded to lat:${ll1.getLat()} lon:${ll1.getLng()}");
  }
}

// def starttime = System.currentTimeMillis();
// def l = new Loader();
// l.load('uk_gaz_with_geo-2011-05-08.csv');
// println("Gaz load completed in ${System.currentTimeMillis() - starttime}ms");


//      if ( ( person_northing != null ) && ( person_easting != null ) && ( person_northing.length() > 0 ) && ( person_easting.length() > 0 ) ) {
//        Double n = new Double(person_northing);
//        Double e = new Double(person_easting);
//        OSRef os1 = new OSRef(e.doubleValue(), n.doubleValue());
//        LatLng ll1 = os1.toLatLng();
//        addField(document, root, "lat",""+ll1.getLat());
//        addField(document, root, "lng",""+ll1.getLng());
//      }


def inites() {
  println("Init");
  
  org.elasticsearch.groovy.node.GNodeBuilder nodeBuilder = new org.elasticsearch.groovy.node.GNodeBuilder()
  
  nodeBuilder.
  println("Construct node settings");
  
  org.elasticsearch.groovy.common.xcontent.GXContentBuilder gxc = new org.elasticsearch.groovy.common.xcontent.GXContentBuilder();
  
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
      type 'feature'
      source {
        gazmap {
          properties {
            featureType {
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

    println("Installed mappings");
}
