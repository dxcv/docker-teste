FROM ubuntu:18.04

RUN apt-get update \
    && apt-get install -y python3-pip -y

COPY . /simulator
WORKDIR /simulator

RUN pip3 install -r ./requirements.txt

CMD ["python3", "simulator.py"]


# docker build --tag simulator .
# docker run --name python-app -p 5000:5000 my-python-app
# docker run --name simulator simulator