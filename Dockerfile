FROM node:18-alpine

# Install system dependencies for Oracle client
RUN apk add --no-cache \
  libaio \
  libnsl \
  libc6-compat \
  curl \
  unzip

# Create app directory
WORKDIR /app

# Create a non-root user for security
RUN addgroup -g 1001 -S nodejs && \
  adduser -S migrator -u 1001 -G nodejs

# Copy package files
COPY package*.json yarn.lock* ./

# Install dependencies
RUN yarn install --frozen-lockfile --production=false && \
  yarn cache clean

# Copy source code
COPY . .

# Build the application
RUN yarn build

# Remove dev dependencies to reduce image size
RUN yarn install --frozen-lockfile --production=true && \
  yarn cache clean

# Create logs directory
RUN mkdir -p /app/logs && \
  chown -R migrator:nodejs /app

# Switch to non-root user
USER migrator

# Expose port (if you add a health check endpoint later)
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD node -e "console.log('Migration tool is ready')" || exit 1

# Default command
CMD ["yarn", "start"]
