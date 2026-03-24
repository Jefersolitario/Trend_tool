FROM energetechnew.azurecr.io/energetech-python-base:latest AS contistreamlitapp

COPY . /app/

WORKDIR /app

RUN pip install -r requirements.txt

ENV PYTHONPATH /app

EXPOSE 8501

ENTRYPOINT ["streamlit", "run", "contistreamlitapp/Conti_Streamlit_App.py", "--server.port=8501", "--server.address=0.0.0.0"]