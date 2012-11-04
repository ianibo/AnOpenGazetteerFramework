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



