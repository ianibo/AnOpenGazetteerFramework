#!/usr/bin/groovy

@GrabResolver(name='es', root='https://oss.sonatype.org/content/repositories/releases')
// @Grab(group='com.gmongo', module='gmongo', version='0.9.2')
@Grapes([
  @Grab(group='net.sf.opencsv', module='opencsv', version='2.0'),
  @Grab(group='org.elasticsearch', module='elasticsearch', version='0.19.0.RC2'),
  @Grab(group='org.elasticsearch', module='elasticsearch-lang-groovy', version='1.1.0')])

import org.apache.log4j.*

LogManager.rootLogger.level = Level.DEBUG

// http://localhost:9200/gaz/_search?q=%22Brincliffe%20Edge%20Road%22&sort=type:desc&pretty=true
def t = new Test();
t.dotest();

println("All done");
