FROM python:3.11-slim

WORKDIR /app
COPY frontend/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY frontend/streamlit_app.py /app/streamlit_app.py

# Disable Streamlit usage analytics to reduce background network requests
RUN mkdir -p /root/.streamlit \
    && printf "[browser]\n" > /root/.streamlit/config.toml \
    && printf "gatherUsageStats = false\n" >> /root/.streamlit/config.toml

ENV STREAMLIT_SERVER_ENABLECORS=false
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_HEADLESS=true

EXPOSE 8501
CMD ["streamlit", "run", "/app/streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
