FROM apache/spark:3.5.0
ENV IVY2_DIR="/tmp/.ivy2"
RUN mkdir -p $IVY2_DIR && chown -R spark:spark $IVY2_DIR
USER spark
ENV PATH="/opt/spark/bin:${PATH}"
WORKDIR /opt/spark
# The entrypoint will be set in docker-compose to run spark-submit