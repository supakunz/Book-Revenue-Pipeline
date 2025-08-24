FROM node:18

RUN npm install -g json-server

WORKDIR /app
COPY ./data_conversion_rate.json /data/data_conversion_rate.json

EXPOSE 3000

CMD ["json-server", "--watch", "/data/data_conversion_rate.json", "--host", "0.0.0.0" ,"--port", "3000"]