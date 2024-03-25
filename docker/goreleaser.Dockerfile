FROM alpine:3.19.0
ENTRYPOINT ["/pg-el-sync"]
COPY pg-el-sync /