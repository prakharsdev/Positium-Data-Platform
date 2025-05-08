FROM jupyter/base-notebook:python-3.10

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''"]
