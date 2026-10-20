# Release image assembled by GoReleaser from the prebuilt binary (dockers_v2).
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
ARG TARGETPLATFORM
COPY ${TARGETPLATFORM}/foreman /usr/local/bin/foreman
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/foreman"]
CMD ["--help"]
