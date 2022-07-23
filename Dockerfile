FROM python:3.10

COPY requirements.txt requirements.txt

RUN pip install -U pip wheel setuptools \
 && pip install -r requirements.txt

ADD . .

EXPOSE 8888

CMD python main.py