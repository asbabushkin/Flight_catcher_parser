FROM python:3.10
COPY requirements.txt requirements.txt
RUN python -m pip install --upgrade pip && pip install -r requirements.txt
WORKDIR /app
COPY data.py  flight_catcher.session injektors.py main.py maintainers.py processors.py selektors.py ./
EXPOSE 8000
ENTRYPOINT ["python3", "main.py"]