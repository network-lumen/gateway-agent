FROM node:18-bullseye-slim

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install --production

COPY src ./src
COPY datasets ./datasets

ENV NODE_ENV=production

CMD ["npm", "run", "start"]
