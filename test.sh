mvn clean install
cd target
rm -rf salida
hadoop jar hadoop-example-1.0-SNAPSHOT.jar mx.iteso.App ~/bigData/obesidad.csv salida
 
