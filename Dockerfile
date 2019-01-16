FROM python:3.6
ENV GDALVERSION "2.1.2"
ENV PIPGDAL "2.1.0"
RUN apt-get update && \
    apt-get install libgdal-dev=${GDALVERSION}* gdal-bin=${GDALVERSION}* -y --no-install-recommends && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    pip install numpy && \
    pip install --global-option=build_ext --global-option="-I/usr/include/gdal" GDAL==${PIPGDAL} && \
    pip install pillow && \
    pip install Jinja2
COPY peerless.py /run/peerless.py
WORKDIR /data
ENTRYPOINT ["python", "/run/peerless.py"]
CMD ["-h"]
