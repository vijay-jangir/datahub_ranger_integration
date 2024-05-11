This is a java code for running tagsync service. 
this service needs to be run periodically using a scheduler (like airflow or crontab)
this will sync all tags b/w datahub to apache ranger services defined in applicatoin.yaml

# how to run
- git clone this repository
- run ```mvn clean package```
- make a copy of src/main/resources/application.yaml and do required changes, save it as tagsync.yaml
- use the generated jar by ```java -jar datahub-ranger-tagsync.jar -Dconfig.location=<path_to_your_config>/tagsync.yaml```
