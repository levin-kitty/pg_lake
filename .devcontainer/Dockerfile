# Extend dev_base with developer-friendly tools
ARG BASE_IMAGE=dev_base

FROM ${BASE_IMAGE}

USER root

# Install useful utilities for development
RUN if command -v dnf &> /dev/null; then \
        dnf -y install \
            procps-ng \
            htop \
            which \
            tree \
            jq \
            vim \
            less \
            net-tools \
            iputils \
            bind-utils \
            && dnf clean all; \
    elif command -v apt-get &> /dev/null; then \
        apt-get update && apt-get install -y \
            procps \
            htop \
            tree \
            jq \
            vim \
            less \
            net-tools \
            iputils-ping \
            dnsutils \
            && rm -rf /var/lib/apt/lists/*; \
    fi

# Upgrade Node.js to 22 LTS (required for Claude Code)
RUN if command -v dnf &> /dev/null; then \
        dnf -y remove nodejs npm || true && \
        curl -fsSL https://rpm.nodesource.com/setup_22.x | bash - && \
        dnf -y install nodejs && dnf clean all; \
    elif command -v apt-get &> /dev/null; then \
        apt-get remove -y nodejs npm || true && \
        curl -fsSL https://deb.nodesource.com/setup_22.x | bash - && \
        apt-get install -y nodejs && rm -rf /var/lib/apt/lists/*; \
    fi

USER postgres

# Keep container running
CMD ["sleep", "infinity"]
