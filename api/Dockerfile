FROM python
COPY ./requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install -r requirements.txt

COPY ./volue_app.py /app/
CMD ["python", "-u", "volue_app.py"]