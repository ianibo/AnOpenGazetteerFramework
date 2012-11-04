#!/usr/bin/groovy

@GrabResolver(name='es', root='https://oss.sonatype.org/content/repositories/releases')
// @Grab(group='com.gmongo', module='gmongo', version='0.9.2')
@Grapes([
  @Grab(group='net.sf.opencsv', module='opencsv', version='2.0'),
  @Grab(group='org.elasticsearch', module='elasticsearch', version='0.19.4'),
  @Grab(group='uk.me.jstott.jcoord', module='jcoord', version='1.0'),
  @Grab(group='org.elasticsearch', module='elasticsearch-lang-groovy', version='1.1.0')])

import org.apache.log4j.*
import uk.me.jstott.jcoord.*;

LogManager.rootLogger.level = Level.DEBUG
log = Logger.getLogger(this.class)

log.debug("ESLoadGaz");

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

