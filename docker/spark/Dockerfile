# SmartCity: Spark 3.5 + dependencias de PySpark MLlib.
# Mantener la misma imagen base de docker/docker-compose.yml.
FROM bitnamilegacy/spark:3.5

USER 0
RUN /opt/bitnami/python/bin/python -m pip install --no-cache-dir \
      "numpy==1.26.4" "scipy==1.13.1" \
    && /opt/bitnami/python/bin/python -c \
      "import numpy, scipy; print('SmartCity Spark ML OK:', numpy.__version__, scipy.__version__)"

# Regresar al usuario predeterminado de Bitnami para los servicios.
USER 1001
