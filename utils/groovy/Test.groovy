#!/usr/bin/groovy

import au.com.bytecode.opencsv.CSVReader
import org.elasticsearch.groovy.client.GClient
import org.elasticsearch.groovy.node.GNode
import org.apache.log4j.*


public class Test {

  public Test() {
  }

  public dotest() {
    println("Initialise");
    GNode esnode = inites();
    GClient esclient = esnode.getClient();
    resolvePlaceName(esclient, "S3 8PZ");
    esnode.stop().close();
    println("All done");
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
  
  def resolvePlaceName(esclient, query_input) {

    println "Resolve place name in ${query_input}"
    def gazresp = [:]
    gazresp.places = []
    gazresp.newq = "";

    // Step 1 : See if the input place name matches a fully qualified place name
    println "exact match q params: ${query_input}"

    def result = search(esclient, query_input, 0, 10);

    // ModifiableSolrParams solr_params = new ModifiableSolrParams();
    // solr_params.set("q", "fqn:\"${query_input}\"")
    // solr_params.set("start", 0);
    // solr_params.set("rows", "10");

    // def response = solrGazBean.query(solr_params);

    // Try and do an exact place name match first of all
    // if ( response.getResults().getNumFound() == 1 ) {
    //   println "Exact place name match..."
    //   def doc = response.getResults().get(0);
    //   def sr = ['lat':doc['centroid_lat'],'lon':doc['centroid_lon'], 'name':doc['place_name'], 'fqn':doc['fqn'], 'type':doc['type'], 'alias':doc['alaias']]
    //   gazresp.places.add(sr)
    // }
    // else {
      // Doing text match on place name...
    //   solr_params.set("q", "place_name:(${query_input}) OR alias:(${query_input})");
    //   solr_params.set("sort", "type desc, score desc");

    //   println "Attempting generic place name match ${solr_params}"
    //   response = solrGazBean.query(solr_params);
    //   response.getResults().each { doc ->
    //     def sr = ['lat':doc['centroid_lat'],'lon':doc['centroid_lon'], 'name':doc['place_name'], 'fqn':doc['fqn'], 'type':doc['type'], 'alias':doc['alaias']]
    //     gazresp.places.add(sr)
    //   }
    // }

    // if ( gazresp.places.size() > 0 ) {
      // Remove any instances of postcode or alias from the query
    //   gazresp.newq = "${query_input}"
    //   gazresp.newq = query_input.replaceAll("${gazresp.places[0].name}","")
    //   gazresp.newq = gazresp.newq.replaceAll("${gazresp.places[0].alias}","")
    // }

    gazresp
  }

  def search(esclient, qry, start, rows) {

    println("Search for ${qry}");

    def search_closure = {
      source {
        from = 0
        size = 10
        query {
          query_string (query: qry)
        }
      }
    }

    def res = esclient.search(search_closure)

    println "Search returned $search.response.hits.totalHits total hits"
    println "First hit course is $search.response.hits[0]"
    //   result.hits = search.response.hits
    //   result.resultsTotal = search.response.hits.totalHits
    res
  }

  
}
