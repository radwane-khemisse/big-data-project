FROM bitnami/spark:3.5.1

USER root
RUN install_packages curl
RUN mkdir -p /opt/bitnami/spark/jars
RUN curl -L -o /opt/bitnami/spark/jars/postgresql-42.7.3.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

USER 1001
