FROM python:3.5-alpine

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY send.py ./

CMD python send.py
