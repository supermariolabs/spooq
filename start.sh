#!/bin/bash

if [[ -z ${MY_CA_BUNDLE} ]]; then
  echo "no CA bundle... nothing to do!"
else
  echo "Updating CAs...\n----------"
  keytool -import -noprompt -cacerts -storepass changeit -alias my -file $MY_CA_BUNDLE
fi

./bin/spooq ${SPOOQ_COMMAND}
