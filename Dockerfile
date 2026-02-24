FROM node:24

WORKDIR /app
COPY package.json ./
COPY src ./src

ENV SHIPPING_HOST=0.0.0.0
ENV SHIPPING_PORT=9000

EXPOSE 9000
CMD ["npm", "run", "start"]
