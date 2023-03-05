FROM python:3.7
RUN pip install git+https://github.com/toluaina/pgsync.git
ENTRYPOINT sleep 5 && bootstrap --config /app/schema.json && pgsync --config /app/schema.json --daemon