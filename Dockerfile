FROM python:3.9
LABEL authors="alfonso"

WORKDIR ./
COPY ./src ./src
COPY ./data ./data
RUN mkdir /results
RUN python -m pip install --no-cache-dir --upgrade pip
RUN python -m pip install --no-cache-dir numpy
RUN python -m pip install --no-cache-dir arm_pyart
ENTRYPOINT ["python", "src/get_data.py"]
#CMD ["python", "src/get_data.py"]
