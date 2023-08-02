FROM python:3.9
LABEL authors="alfonso"

WORKDIR ./
COPY ./src ./src
COPY ./data ./data
RUN mkdir /results
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir arm_pyart

CMD ["python", "src/get_data.py"]
