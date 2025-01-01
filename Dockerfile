
FROM python:3.11-slim

WORKDIR /usr/src/app
COPY . .

# download requirements
RUN pip install --no-cache-dir -r requirements.txt 

# CMD ["python", "./main.py"]
CMD ["sh", "-c", "python ./main.py & tail -f /dev/null"]