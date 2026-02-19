FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire data_generator package into /app/data_generator
COPY . /app/data_generator/

# Run the data generator as a module, running the main function in data_generator/main.py
# Using -m parameter to run the main.py file as a module, because main.py uses relative imports to import from other files in the data_generator package
CMD ["python", "-m", "data_generator.main"]
