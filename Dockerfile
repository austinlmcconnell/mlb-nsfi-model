FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
COPY .streamlit /app/.streamlit
COPY . /app/
EXPOSE 8501
CMD ["streamlit", "run", "model.py", "--server.address=0.0.0.0"]