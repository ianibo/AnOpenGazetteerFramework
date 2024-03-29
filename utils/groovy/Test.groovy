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
    resolvePlaceName(esclient, "S3 8PZ, Paradise Street, SHEFFIELD, South Yorkshire, England, UK");
    resolvePlaceName(esclient, "Ecclesall Road");
    resolvePlaceName(esclient, "Main Street");
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

    def result = search(esclient, "fqn.orig:\"${query_input}\"", 0, 10);

    if ( result.response.hits.totalHits == 1 ) {
      System.out.println("Exact match on fqn for ${query_input}");
      def sr = [
           'lat':result.response.hits[0].source.location?.lat,
           'lon':result.response.hits[0].source.location?.lon,
           'name':result.response.hits[0].source.placeName,
           'fqn':result.response.hits[0].source.fqn,
           'type':result.response.hits[0].source.type
      ]
      gazresp.places.add(sr)
    }
    else {
      System.out.println("No exact fqn match for ${query_input}, try sub match");
      result = search(esclient, "fqn:\"${query_input}\"", 0, 10);
      System.out.println("Got ${result.response.hits} hits...");
      if ( result.response.hits.totalHits > 0 ) {
        println("Iterating hits...");
        result.response.hits.each { hit -> 
          println("Adding ${hit.source}");
          gazresp.places.add([
           'lat':hit.source.location?.lat,
           'lon':hit.source.location?.lon,
           'name':hit.source.placeName,
           'fqn':hit.source.fqn,
           'type':hit.source.type
          ] )
        }
        println("Done Iterating hits...");
      }
    }

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
    println "Search returned $res.response.hits.totalHits total hits"
    // println "First hit course is $res.response.hits[0]"
    // result.hits = search.response.hits
    // result.resultsTotal = search.response.hits.totalHits


    //   result.hits = search.response.hits
    //   result.resultsTotal = search.response.hits.totalHits
    res
  }

  
}
