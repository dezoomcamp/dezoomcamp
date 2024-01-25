FROM python:3.9

RUN pip install pandas
RUN pip install Pyarrow

WORKDIR /app
COPY simple_py_file.py simple_py_file.py 

ENTRYPOINT ["python","simple_py_file.py"]