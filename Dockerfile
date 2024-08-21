FROM python:3.10.14
WORKDIR /flight_parser
COPY requirements.txt requirements.txt
RUN python -m pip install --upgrade pip && pip install -r requirements.txt
COPY data.py data.py
COPY injektors.py injektors.py
COPY main.py main.py
COPY maintainers.py maintainers.py
COPY processors.py processors.py
COPY selektors.py selektors.py
COPY tg_bot.py tg_bot.py
ENTRYPOINT ["python3", "main.py"]
