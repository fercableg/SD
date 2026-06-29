FROM apache/spark:3.5.0
ENV PATH="/opt/spark/bin:${PATH}"


USER root
RUN mkdir -p /home/spark/.ivy2 && chown -R spark:spark /home/spark/.ivy2

USER spark
WORKDIR /opt/spark

COPY spark_job.py /opt/spark/jobs/