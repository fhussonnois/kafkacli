#!/bin/bash

set -e

base_dir=`dirname $(readlink -f $0)`

echo $base_dir

if [ "x$GO_HOME" = "x" ]; then
	export GO_HOME=/usr/local/go
fi

KAFKA_CONNECT_CLI=github.com/fhussonnois/kafkacli/cmd/kafkaconnectcli
SCHEMA_REGISTRY_CLI=github.com/fhussonnois/kafkacli/cmd/schemaregistrycli

GO=${GO_HOME}/bin/go
BUILD_PATH=${base_dir}/build
DIST_PATH=${base_dir}/dist

function clean {
    rm -rf ${BUILD_PATH}
}

function compile {
    GO_OS=$1
    GO_ARCH=$2
    DIST=kafka-cli-${GO_OS}-${GO_ARCH}
	echo "Compiling binaries for...$1/$2"
	env GOOS=${GO_OS} GOARCH=${GO_ARCH} ${GO} build -o ${BUILD_PATH}/${DIST}/kafka-connect-cli ${KAFKA_CONNECT_CLI}
	env GOOS=${GO_OS} GOARCH=${GO_ARCH} ${GO} build -o ${BUILD_PATH}/${DIST}/schema-registry-cli ${SCHEMA_REGISTRY_CLI}

	cp ${base_dir}/README.md ${BUILD_PATH}/${DIST}
	cp ${base_dir}/LICENSE ${BUILD_PATH}/${DIST}
    cp ${base_dir}/AUTHORS.txt ${BUILD_PATH}/${DIST}
    cp ${base_dir}/CONTRIBUTING.md ${BUILD_PATH}/${DIST}

    echo "Building archive ${base_dir}/dist/${DIST}.tar.gz"
    (cd ${BUILD_PATH}; tar -czvf ${DIST_PATH}/${DIST}.tar.gz ${DIST})
    (cd ${DIST_PATH}; sha1sum ${DIST}.tar.gz > ${DIST}.tar.gz.sha1)

    echo "Building archive ${base_dir}/dist/${DIST}.zip"
    (cd ${BUILD_PATH}; zip -r ${DIST_PATH}/${DIST}.zip ${DIST})
    (cd ${DIST_PATH}; sha1sum ${DIST}.zip > ${DIST}.zip.sha1)
}

if [ ! -d "${DIST_PATH}" ]; then
    mkdir ${DIST_PATH}
else
    rm -rf ${DIST_PATH}
fi

compile linux amd64
compile linux 386
compile darwin amd64
compile darwin 386
compile windows amd64
compile windows 386

clean

exit 0;


