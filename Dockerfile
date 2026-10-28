FROM node:24-trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    procps \
    git \
    curl \
    ca-certificates \
    python3 \
    ripgrep \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

# Runtime libs for the Playwright browsers cached in the pi-home volume
# (~/.cache/ms-playwright). Without these the binaries exist but every launch
# dies on "libglib-2.0.so.0: cannot open shared object file"; the list is
# exactly `ldd chrome-headless-shell | grep 'not found'`. Fonts so screenshots
# are legible rather than tofu.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libatspi2.0-0 \
    libdbus-1-3 libgbm1 libdrm2 libx11-6 libxcomposite1 libxdamage1 libxext6 \
    libxfixes3 libxrandr2 libxcb1 libxkbcommon0 libasound2 \
    fontconfig fonts-liberation fonts-dejavu-core \
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
