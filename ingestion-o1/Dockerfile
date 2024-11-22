# use an official python runtime as a parent image
FROM python:3.9-slim

# set environment variables
ENV PYTHONUNBUFFERED=1

# set the working directory in the container
WORKDIR /app

# copy requirements.txt into the container at /app
COPY requirements.txt /app/

# install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# copy the rest of the application code into the container at /app
COPY src/ /app/src/
COPY .env /app/

# expose port if needed (optional, remove if not required)
# expose 8000

# command to run on container start
CMD ["python", "src/main.py"]