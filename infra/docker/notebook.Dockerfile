# infra/docker/notebook.Dockerfile

FROM python:3.11-slim

# Install Python packages
RUN pip install --no-cache-dir \
    jupyterlab \
    duckdb \
    pandas \
    pyarrow \
    fsspec \
    s3fs \
    pyyaml \
    matplotlib \
    seaborn \
    plotly

WORKDIR /opt/de_project

EXPOSE 8888
