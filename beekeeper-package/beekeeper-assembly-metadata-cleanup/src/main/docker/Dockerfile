FROM openjdk:11-jdk
RUN curl "https://aws-cli.s3.amazonaws.com/awscli-bundle.zip" -o "awscli-bundle.zip"
RUN unzip awscli-bundle.zip
RUN ./awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws
ARG PORT
EXPOSE $PORT
ARG BEEKEEPER_VERSION
COPY /target/beekeeper-metadata-cleanup-${BEEKEEPER_VERSION}-bin.tgz /usr/src/app/beekeeper-metadata-cleanup-${BEEKEEPER_VERSION}-bin.tgz
WORKDIR /usr/src/app
RUN tar -xf beekeeper-metadata-cleanup-${BEEKEEPER_VERSION}-bin.tgz
WORKDIR /usr/src/app/beekeeper-metadata-cleanup-${BEEKEEPER_VERSION}
ENV BEEKEEPER_VERSION=${BEEKEEPER_VERSION}
ENTRYPOINT ["/bin/bash", "-c", "exec /usr/src/app/beekeeper-metadata-cleanup-${BEEKEEPER_VERSION}/bin/startup.sh"]
