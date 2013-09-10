FLAGS="-ea -server"
CLASSPATH="-cp bin:lib/logback-classic-1.0.13.jar:lib/logback-core-1.0.13.jar:lib/slf4j-api-1.7.5.jar"
LOGGING="-Dlogback.configurationFile=logback.xml"
OPTS="${FLAGS} ${LOGGING} ${CLASSPATH}"