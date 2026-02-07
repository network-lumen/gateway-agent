FROM node:20-bullseye-slim

WORKDIR /app/indexer

COPY indexer/package*.json ./
RUN npm ci --omit=dev --no-audit --no-fund

COPY indexer/. .

ENV NODE_ENV=production

CMD ["npm", "run", "indexer"]
