FROM node:20-bullseye-slim

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund

COPY src ./src
COPY datasets ./datasets

ENV NODE_ENV=production

CMD ["npm", "run", "start"]
