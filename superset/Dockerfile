FROM apache/superset:3.0.2

USER root

COPY --chmod=777 superset_config.py /app/
COPY --chmod=777 superset_init.sh /superset_init.sh
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

USER superset
ENTRYPOINT ["/superset_init.sh"]