FROM alpine:3.19.0

COPY pg-el-sync /bin

ENV USER=nonroot
ENV UID=10001

# Create passwd empty file
RUN echo "$USER:x:$UID:$UID::/nonexistent:/sbin/nologin" > /etc/passwd
RUN echo "$USER:x:$UID:" > /etc/group

# Fetch dump-init
RUN wget -O /bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.5/dumb-init_1.2.5_x86_64
RUN chmod +x /bin/dumb-init


USER $USER:$USER
ENTRYPOINT ["/bin/dumb-init", "--"]
CMD ["pg-el-sync","listen"]