LOG_CONF="log4j2.xml"

if [[ -z "$LOG_CONF" ]]; then
    java -Dlog4j.configurationFile="$LOG_CONF" -cp "*" nl.hayovanloon.gcp.sql2gcloud.Runner $@
else 
    java -cp "*" nl.hayovanloon.gcp.sql2gcloud.Runner $@
fi