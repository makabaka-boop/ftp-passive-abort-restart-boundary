FROM node:20-alpine

WORKDIR /app
COPY ftpd.js ./

ENV FTPD_PORT=2121 \
    FTPD_ACCEPT_TIMEOUT_MS=10000 \
    PASV_PORT_MIN=30000 \
    PASV_PORT_MAX=30009

EXPOSE 2121
EXPOSE 30000-30009

USER node
# 单进程:node 直接作为容器主进程运行
CMD ["node", "ftpd.js"]
