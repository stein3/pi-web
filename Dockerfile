FROM node:24-bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    procps \
    git \
    curl \
    ca-certificates \
    python3 \
    ripgrep \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

# --ignore-scripts per pi docs.
RUN npm install -g --ignore-scripts @earendil-works/pi-coding-agent@1.3.0
# Browser workspace: spawns `pi --mode rpc` children, one per in-page tab.
# Needs `pi` on PATH (above) and Node >=22.19 (image is 24).
RUN npm install -g --ignore-scripts pi-tau-web-server@1.1.8

COPY web/pi-web.mjs /app/pi-web.mjs
RUN chown -R node:node /app
# pi-web imports tau's session manager as a library. ESM can't resolve a global
# npm install, so symlink it into /app/node_modules (its own deps resolve via
# the real path under /usr/local/lib/node_modules). Created AFTER the chown so
# `chown -R` never follows it into the global package tree.
RUN mkdir -p /app/node_modules \
    && ln -s /usr/local/lib/node_modules/pi-tau-web-server /app/node_modules/pi-tau-web-server

# Pi's config dir ~/.pi/agent (credentials, settings, sessions) and ~/.ssh must exist with the
# right owner+mode before a mount lands on them, or they are created root-owned.
# ssh also refuses keys unless ~/.ssh is 0700 and keys are 0600-owned-by-us.
RUN mkdir -p /home/node/.pi/agent /home/node/.ssh \
    && chmod 700 /home/node/.ssh \
    && chown -R node:node /home/node/.pi /home/node/.ssh

WORKDIR /workspace
USER node
ENTRYPOINT ["pi"]
