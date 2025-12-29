FROM node:18-bullseye-slim

WORKDIR /app/indexer

COPY indexer/package*.json ./
RUN npm install --omit=dev

COPY indexer/. .

ENV NODE_ENV=production

CMD ["npm", "run", "indexer"]
