FROM python:3.10
COPY requirements.txt requirements.txt
RUN python -m pip install --upgrade pip && pip install -r requirements.txt
WORKDIR /app
COPY data.py injektors.py main.py maintainers.py processors.py selektors.py wsgi.py ./
ENTRYPOINT ["python3", "main.py"]