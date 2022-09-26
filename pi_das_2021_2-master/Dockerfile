FROM golang:1.16-alpine
WORKDIR /app
COPY web/public ./web/public
COPY .env ./
COPY middleware ./middleware
CMD [ "/middleware" ]
