#!/usr/bin/env bash

set -e

declare -r samples_sources="${PWD}/samples"
declare -r akka_edge_docs_attachments="${PWD}/akka-edge-docs/src/main/paradox/attachments"
declare -r akka_distributed_cluster_docs_attachments="${PWD}/akka-distributed-cluster-docs/src/main/paradox/attachments"
declare -r target_temporal_attachments="${PWD}/target/akka-edge-docs/_attachments"

declare -r temporal_folder="${PWD}/target/zips"

function sed_command() {
   local platform="$(uname -s | tr '[:upper:]' '[:lower:]')"

   if [ "${platform}" != "darwin" ]; then
      echo "sed"
   else
      # using gnu-sed on Mac
      echo "gsed"
   fi
}

## Remove the tags used by Paradox snippets from the codebase in the current folder
function removeTags() {
   ## remove tags from code
   find . -type f -print0 | xargs -0 $(sed_command) -i "s/\/\/ #.*//g"
}


## Cleanup the temporal folder from previous executions
function prepareTemporalFolder() {
   rm -rf ${temporal_folder}
   mkdir -p ${temporal_folder}
}

## Copy a folder with some code into the temporal folder. The 
## copied folder will be renamed to the folder name we want the 
## user to see when unzipping the file.
##   source_name -> folder in `examples`
##   target_name ->  folder name the user should see (must not use a numeric prefix of a laguage suffix)
function fetchProject() {
   source_name=$1
   target_name=$2
   echo "Fetching content from [$1] to [$2]"
   cp -a ${source_name} ${temporal_folder}/${target_name}
   rm -rf ${temporal_folder}/${target_name}/target
}

## Zip the contents in $temporal_folder and create the 
## attachment file (aka, the zip file on the appropriate location)
function zipAndAttach() {
   zip_name=$1
   temporal_attachments=$2
   echo "Preparing zip $1"
   pushd ${temporal_folder}
   removeTags
   zip --quiet -r ${zip_name} *
   cp ${zip_name} ${temporal_attachments}
   echo "Prepared attachment at ${zip_name}"
   popd
}

mkdir -p ${akka_edge_docs_attachments}
mkdir -p ${akka_distributed_cluster_docs_attachments}
mkdir -p ${target_temporal_attachments}

## akka-distributed-cluster-docs Scala zip files
prepareTemporalFolder
fetchProject ${samples_sources}/grpc/shopping-cart-service-scala shopping-cart-service
fetchProject ${samples_sources}/grpc/shopping-analytics-service-scala shopping-analytics-service
zipAndAttach ${akka_distributed_cluster_docs_attachments}/shopping-scala.zip ${target_temporal_attachments}

prepareTemporalFolder
fetchProject ${samples_sources}/replicated/shopping-cart-service-scala replicated-shopping-cart-service
zipAndAttach ${akka_distributed_cluster_docs_attachments}/replicated-shopping-scala.zip ${target_temporal_attachments}

## akka-distributed-cluster-docs Java zip files
prepareTemporalFolder
fetchProject ${samples_sources}/grpc/shopping-cart-service-java shopping-cart-service
fetchProject ${samples_sources}/grpc/shopping-analytics-service-java shopping-analytics-service
zipAndAttach ${akka_distributed_cluster_docs_attachments}/shopping-java.zip ${target_temporal_attachments}

prepareTemporalFolder
fetchProject ${samples_sources}/replicated/shopping-cart-service-java replicated-shopping-cart-service
zipAndAttach ${akka_distributed_cluster_docs_attachments}/replicated-shopping-java.zip ${target_temporal_attachments}

## akka-edge-docs Scala zip files
prepareTemporalFolder
fetchProject ${samples_sources}/grpc/local-drone-control-scala local-drone-control
fetchProject ${samples_sources}/grpc/restaurant-drone-deliveries-service-scala restaurant-drone-deliveries-service
zipAndAttach ${akka_edge_docs_attachments}/drone-scala.zip ${target_temporal_attachments}

## akka-edge-docs Java zip files
prepareTemporalFolder
fetchProject ${samples_sources}/grpc/local-drone-control-java local-drone-control
fetchProject ${samples_sources}/grpc/restaurant-drone-deliveries-service-java restaurant-drone-deliveries-service
zipAndAttach ${akka_edge_docs_attachments}/drone-java.zip ${target_temporal_attachments}

## akka-edge-docs Scala zip files
prepareTemporalFolder
fetchProject ${samples_sources}/grpc/iot-service-scala iot-service-scala
fetchProject ${samples_sources}/grpc/iot-service-java iot-service-java
fetchProject ${samples_sources}/grpc/iot-service-rs iot-service-rs
zipAndAttach ${akka_edge_docs_attachments}/iot-service.zip ${target_temporal_attachments}
