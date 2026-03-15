FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY streamlit_app.py .
EXPOSE 8080
CMD ["bash","-lc","streamlit run streamlit_app.py --server.address 0.0.0.0 --server.port ${PORT:-8080} --server.headless true"]
