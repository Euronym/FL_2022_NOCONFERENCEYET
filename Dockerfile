FROM python:3.9

COPY requirements.txt requirements.txt

CMD ["python", "distributed.py"]