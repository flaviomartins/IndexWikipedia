IndexWikipedia
==============

A simple utility to index wikipedia dumps using Lucene.


Usage:

install java (JDK) if needed
install maven if needed
grap your wikipedia dump: http://en.wikipedia.org/wiki/Wikipedia:Database\_download
mvn compile
mvn exec:java -Dexec.args="yourdump someoutputdirectory"

Actual example:

nohup mvn compile && nohup mvn -e exec:java -Dexec.args="/home/dlemire/WikipediaDump/enwiki-20130102-pages-articles.xml.bz2 /home/dlemire/WikipediaIndex" &

