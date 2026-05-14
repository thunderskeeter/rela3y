FROM node:20-alpine

WORKDIR /app

COPY backend/package*.json ./backend/
RUN cd backend && npm ci --omit=dev

COPY backend ./backend
COPY frontend ./frontend

ENV NODE_ENV=production
ENV PORT=3001
ENV DEV_MODE=false

WORKDIR /app/backend
EXPOSE 3001

CMD ["node", "server.js"]

